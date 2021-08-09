"""NESDIS Satellite Cloud Product.

This service emits an outer join between the NESDIS Satellite Cloud Product
and available METAR cloud reports.  The NESDIS product is resampled to
match the closest METAR in time.  The column names in the response are
suffixed to include the SCP source code for that observation.  For example,
the field ``mid_1`` represents the mid value from the Goes East Sounder. The
``_2`` value is the Goes West Sounder and ``_3`` value is the Goes Imager. A
given site may have 1 or more of those 3 potential options."""
import datetime

from fastapi import Query, Response
from pandas.io.sql import read_sql
import pandas as pd
from pyiem.util import utc
from ..util import get_dbconn


def handler(station, date):
    """Handle the request, return dict"""
    station = f"K{station}" if len(station) == 3 else station
    station3 = station[1:] if station.startswith("K") else station
    sts = utc(date.year, date.month, date.day, 0, 0)
    ets = sts + datetime.timedelta(hours=24)
    dbconn = get_dbconn("asos")
    # Get METARs
    obs = read_sql(
        "SELECT valid at time zone 'UTC' as utc_valid, metar, skyc1, skyl1, "
        "skyc2, skyl2, skyc3, skyl3, skyc4, skyl4 "
        "from alldata where station = %s and valid >= %s "
        "and valid < %s and report_type = 2 ORDER by valid ASC",
        dbconn,
        index_col=None,
        params=(station3, sts, ets),
    )
    # Get SCP
    scp = read_sql(
        "SELECT valid at time zone 'UTC' as utc_scp_valid, mid, high, "
        "cldtop1, cldtop2, eca, source from scp_alldata "
        "where station = %s and valid >= %s "
        "and valid < %s ORDER by valid ASC",
        dbconn,
        index_col=None,
        params=(station, sts, ets),
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
    return df.to_json(orient="table", index=False, default_handler=str)


def factory(app):
    """Generate."""

    @app.get("/scp.json", description=__doc__)
    def service(
        station: str = Query(..., max_length=5, min_length=3),
        date: datetime.date = Query(..., description="UTC date of interest"),
    ):
        """Replaced above by __doc__."""
        return Response(handler(station, date), media_type="application/json")

    service.__doc__ = __doc__
