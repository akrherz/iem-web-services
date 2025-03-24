"""NESDIS Satellite Cloud Product.

This service emits an outer join between the NESDIS Satellite Cloud Product
and available METAR cloud reports.  The NESDIS product is resampled to
match the closest METAR in time.  The column names in the response are
suffixed to include the SCP source code for that observation.  For example,
the field ``mid_1`` represents the mid value from the Goes East Sounder. The
``_2`` value is the Goes West Sounder and ``_3`` value is the Goes Imager. A
given site may have 1 or more of those 3 potential options."""

from datetime import date as dateobj
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
from fastapi import APIRouter, Query
from pyiem.database import sql_helper

from ..util import deliver_df, get_sqlalchemy_conn

router = APIRouter()


def handler(station, dt, tz: str):
    """Handle the request, return dict"""
    station = f"K{station}" if len(station) == 3 else station
    station3 = station[1:] if station.startswith("K") else station
    tzinfo = ZoneInfo(tz)
    sts = datetime(dt.year, dt.month, dt.day, tzinfo=tzinfo)
    ets = sts + timedelta(hours=24)
    with get_sqlalchemy_conn("asos") as dbconn:
        # Get METARs
        obs = pd.read_sql(
            sql_helper("""
            SELECT valid at time zone 'UTC' as utc_valid,
            to_char(valid at time zone :tz, 'YYYY-MM-DDThh24:MI:SS')
                 as local_valid,
            metar, skyc1, skyl1,
            skyc2, skyl2, skyc3, skyl3, skyc4, skyl4
            from alldata where station = :station3 and valid >= :sts
            and valid < :ets and report_type != 1 ORDER by valid ASC
            """),
            dbconn,
            index_col=None,
            params={"station3": station3, "sts": sts, "ets": ets, "tz": tz},
        )
        # Get SCP
        scp = pd.read_sql(
            sql_helper("""
            SELECT valid at time zone 'UTC' as utc_scp_valid,
            mid, high,
            cldtop1, cldtop2, eca, source from scp_alldata
            where station = :station and valid >= :sts
            and valid < :ets ORDER by valid ASC
                 """),
            dbconn,
            index_col=None,
            params={"station": station, "sts": sts, "ets": ets},
        )
    # Figure out how many unique sources we have
    df = None
    for source in scp["source"].unique():
        df2 = (
            scp[scp["source"] == source]
            .copy()
            .set_index("utc_scp_valid")
            .drop("source", axis=1)
        )
        df2.columns = [f"{s}_{source}" for s in df2.columns]
        if df is None:
            df = df2
            continue
        # Join
        df = df.join(df2)
    # Case 1, we have scp, but no obs
    if obs.empty and df is not None:
        pass
    # Case 2, we have obs, but no scp
    elif df is None:
        df = obs
    # Case 3, we have both, hopefully
    else:
        df = df.reset_index()
        # Reindex scp to match obs
        df = pd.merge_asof(
            df,
            obs,
            right_on="utc_valid",
            left_on="utc_scp_valid",
            direction="nearest",
        )
    return df


@router.get(
    "/scp.json",
    description=__doc__,
    tags=[
        "nws",
    ],
)
def service(
    station: str = Query(..., max_length=5, min_length=3),
    date: dateobj = Query(..., description="Date of interest"),
    tz: str = Query("UTC", description="Timezone to report timestamps in"),
):
    """Replaced above by __doc__."""
    df = handler(station, date, tz)
    return deliver_df(df, "json")


service.__doc__ = __doc__
