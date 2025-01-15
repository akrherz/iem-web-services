"""ASOS Interval Summary Service.

This service attempts to compute summary values over arbitrary time intervals
for stations within the ASOS network.  This is based on the hourly observations
reported in METAR format.  This is not an exact science due to all kinds of
complexities.  Many life choices are made to attempt to do the right thing
here.  To prevent a DOS, a total request size score of 100 is enforced
(number of stations times number of days).

It is highly recommended that you only request for periods with minute values
at the top of the hour.  Anything else will get you an even more nebulous
result.

Trace values are encoded as 0.0001 inches.  If you specify multiple stations,
the results are returned in the order of the stations requested.

Life Choice 1.  If you request a beginning timestamp at the top of the hour,
the service will look back 10 minutes from that timestamp to capture any
temperature observations that were made in that window.  The reason is that
this observation is typically considered the "synoptic" observation for the
hour.  The precipitation report for this observation is not considered, since
it represents the previous hour.

Life Choice 2.  In the case of ties for max wind speed or gust, the most recent
occurence is used.
"""

from datetime import datetime, timedelta, timezone

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from pyiem.reference import ISO8601
from sqlalchemy import text

from ..models import SupportedFormatsNoGeoJSON
from ..models.asos_interval_summary import AISSchema
from ..util import deliver_df, get_sqlalchemy_conn

router = APIRouter()


def get_df(stations, sts: datetime, ets: datetime) -> pd.DataFrame:
    """Figure out how to get the data being requested."""
    effsts = sts
    if sts.minute == 0:
        effsts = sts - timedelta(minutes=10)
    with get_sqlalchemy_conn("asos") as pgconn:
        obs = pd.read_sql(
            text("""
                SELECT station, valid at time zone 'UTC' as utc_valid,
                tmpf, p01i, sknt, gust, drct, peak_wind_gust, peak_wind_drct,
                peak_wind_time at time zone 'UTC' as peak_wind_time,
                max_tmpf_6hr, min_tmpf_6hr, report_type from alldata WHERE
                station = ANY(:stations) and valid > :sts and valid <= :ets
                and report_type in (3, 4) order by valid asc
                """),
            pgconn,
            params={"stations": stations, "sts": effsts, "ets": ets},
            index_col=None,
            parse_dates=["utc_valid", "peak_wind_time"],
        )
    # Ensure that the utc_valid is localized
    if not obs.empty:
        for col in ["peak_wind_time", "utc_valid"]:
            obs[col] = obs[col].dt.tz_localize("UTC")
    return obs


def compute(station: str, obs: pd.DataFrame, sts) -> dict:
    """Do the magic work."""
    res = {
        "station": station,
        "max_tmpf": None,
        "min_tmpf": None,
        "total_precip_in": None,
        "obs_count": 0,
        "max_speed_kts": None,
        "max_gust_kts": None,
        "max_speed_drct": None,
        "max_gust_drct": None,
        "max_speed_time_utc": None,
        "max_gust_time_utc": None,
    }
    if obs.empty:
        return res
    # Max sknt is straight forward
    if obs["sknt"].max() > 0:
        row = obs.sort_values(["sknt", "utc_valid"], ascending=False).iloc[0]
        res["max_speed_kts"] = row["sknt"]
        res["max_speed_drct"] = row["drct"]
        res["max_speed_time_utc"] = row["utc_valid"].strftime(ISO8601)
    # Max gust is a bit more complex
    if obs["gust"].max() > 0:
        row = obs.sort_values(["gust", "utc_valid"], ascending=False).iloc[0]
        res["max_gust_kts"] = row["gust"]
        res["max_gust_time_utc"] = row["utc_valid"].strftime(ISO8601)
        if obs["peak_wind_gust"].max() >= obs["gust"].max():
            row = obs.sort_values(
                ["peak_wind_gust", "peak_wind_time"], ascending=False
            ).iloc[0]
            res["max_gust_kts"] = row["peak_wind_gust"]
            res["max_gust_drct"] = row["peak_wind_drct"]
            res["max_gust_time_utc"] = row["peak_wind_time"].strftime(ISO8601)
    res["max_tmpf"] = obs["tmpf"].max()
    res["min_tmpf"] = obs["tmpf"].min()
    res["obs_count"] = len(obs.index)
    # Need to exclude any synoptic obs outside of the window
    available = obs[obs["utc_valid"] >= sts]
    res["total_precip_in"] = (
        available[["utc_valid", "p01i"]]
        .groupby(available["utc_valid"].dt.strftime("%Y%m%d%H"))
        .max()["p01i"]
        .fillna(0)
        .sum()
    )
    # Ensure that trace values don't mess things up
    if res["total_precip_in"] > 0.001:
        res["total_precip_in"] = round(res["total_precip_in"], 2)
    # Now we get cute, 6 hour max/min reports can be used, if they are within
    # the period of interest
    # Life choice to trim this to 5 hours and 50 minutes, to muck synop time
    sts6 = obs["utc_valid"] - pd.Timedelta("350 minutes")
    available = obs[sts6 >= pd.Timestamp(sts)]
    if not available.empty:
        maxval = available["max_tmpf_6hr"].max()
        if not pd.isna(maxval):
            res["max_tmpf"] = max(res["max_tmpf"], maxval)
        minval = available["min_tmpf_6hr"].min()
        if not pd.isna(minval):
            res["min_tmpf"] = min(res["min_tmpf"], minval)

    return res


@router.get(
    "/asos_interval_summary.{fmt}",
    response_model=AISSchema,
    description=__doc__,
    tags=[
        "iem",
    ],
)
def service(
    fmt: SupportedFormatsNoGeoJSON,
    station: str = Query(
        ...,
        description=(
            "Single station identifier or comma separated list, each "
            "identifier is three (K-sites) or four characters long."
        ),
        max_length=1000,
        min_length=3,
    ),
    sts: datetime = Query(
        ...,
        description="UTC Start Timestamp, best to be top of the hour",
    ),
    ets: datetime = Query(
        ...,
        description="UTC Inclusive End Timestamp, best to be top of the hour",
    ),
):
    """Replaced above with module __doc__"""
    sts = sts.replace(tzinfo=timezone.utc)
    ets = ets.replace(tzinfo=timezone.utc)
    stations = [x.strip()[:4].upper() for x in station.split(",")]
    score = len(stations) * (ets - sts).days
    if score > 100:
        raise HTTPException(
            status_code=400, detail="Request too large, please reduce"
        )
    obs = get_df(stations, sts, ets)
    if obs.empty:
        raise HTTPException(
            status_code=404, detail="No data found for request"
        )
    rows = []
    for sid in stations:
        rows.append(compute(sid, obs[obs["station"] == sid], sts))
    return deliver_df(pd.DataFrame(rows), fmt)
