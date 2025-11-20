"""Model Output Statistics Service.

This service provides the atomic data from the text station MOS that the NWS
issues.  Depending on the date, the supported `model=` values are AVN, GFS,
ETA, NAM, NBS, NBE, ECM, LAV, and MEX.  The variable names generally match the
abbreviations found in the raw text files, but a few names are rectified to
match between various models.  `x_n` is translated to `n_x`, `wnd` to `wsp`,
and `wgs` to `gst`.

There is an additional quirk to the GFS LAMP (LAV) guidance.
The model `runtime` found in the raw files has a timestamp of 30 minutes after
the hour for reasons I am unsure.  This is rectified back to the top of the
hour.  For example, use `12:00Z` for the 12z run instead of `12:30Z`.

The NBE and NBS MOS data is saved every hour, but then only the 1, 7, 13, and
19 Z runs are saved after 7 days have passed (to save space in the database).
The data found within the NBX is included with the NBE.

This service will emit `404` HTTP status codes if no data is found for the
requested station and model.
"""

from datetime import datetime, timedelta, timezone
from typing import List

import pandas as pd
from fastapi import APIRouter, HTTPException, Query, Response
from pyiem.database import sql_helper
from pyiem.util import utc

from ..models import SupportedFormatsNoGeoJSON
from ..reference import MEDIATYPES
from ..util import cache_control, get_sqlalchemy_conn

MODEL_DOMAIN = ["AVN", "GFS", "ETA", "NAM", "NBS", "NBE", "ECM", "LAV", "MEX"]
COLUMNS = (
    "station,model,runtime,ftime,n_x,tmp,dpt,cld,wdr,wsp,p06,p12,"
    "q06,q12,t06,t12,snw,cig,vis,obv,poz,pos,typ,sky,swh,lcb,"
    "i06,slv,s06,pra,ppl,psn,pzr,t03,gst,q24,p24,t24,ccg,ppo,pco,lp1,lc1,cp1,"
    "cc1,s12,i12,s24,pzp,prs,txn"
).split(",")
router = APIRouter()
# Save some database queries
LATEST_RUNTIME_CACHE = {}


def find_runtime(station: list, model: str) -> datetime:
    """Figure out what our latest runtime is."""
    # Check our cache
    if model in LATEST_RUNTIME_CACHE:
        (settime, runtime) = LATEST_RUNTIME_CACHE[model]
        if settime > utc() - timedelta(minutes=20):
            return runtime
    with get_sqlalchemy_conn("mos") as engine, engine.begin() as conn:
        res = conn.execute(
            sql_helper(
                "SELECT max(runtime) from alldata WHERE model = :model and "
                "station = ANY(:stations) and "
                "runtime > now() - '48 hours'::interval"
            ),
            {"model": model, "stations": station},
        )
        rows = res.fetchone()
        if not rows:
            raise HTTPException(
                404,
                detail=(
                    "For the provided station(s) and lack of runtime= value, "
                    "this service could not find any recent MOS data for the "
                    "provided model over the past 48 hours."
                ),
            )
        runtime = rows[0]
        LATEST_RUNTIME_CACHE[model] = (utc(), runtime)
    return runtime


def handler(station: list, model: str, runtime: datetime, fmt: str) -> str:
    """Handle the request."""
    if runtime is None:
        runtime = find_runtime(station, model)

    # Ready to get the data!
    with get_sqlalchemy_conn("mos") as conn:
        df = pd.read_sql(
            sql_helper(
                """
                SELECT *, t06_1 ||'/'||t06_2 as t06, t12_1 ||'/'|| t12_2
                    as t12,
                runtime at time zone 'UTC' as runtime_utc,
                ftime at time zone 'UTC' as ftime_utc
                from alldata where model = :model and station = ANY(:ids) and
                runtime = :runtime ORDER by station ASC, ftime ASC
                """
            ),
            conn,
            params={"model": model, "ids": station, "runtime": runtime},
            index_col=None,
        )
    if df.empty:
        raise HTTPException(
            404,
            detail=(
                "Database query found no results for the provided station(s) "
                ", model, and runtime.  Please review that your stations are "
                "four character ICAOs in the case of airports."
            ),
        )
    # Hacky to work around string formatting issue with pandas 1.4.0
    df["runtime"] = df["runtime_utc"].dt.strftime("%Y-%m-%d %H:%M")
    df["ftime"] = df["ftime_utc"].dt.strftime("%Y-%m-%d %H:%M")

    if fmt == SupportedFormatsNoGeoJSON.txt:
        return df[COLUMNS].to_csv(index=False)
    # Implement our 'table-schema' option
    return df.to_json(orient="table", default_handler=str)


@router.get(
    "/mos.{fmt}",
    description=__doc__,
    tags=[
        "nws",
    ],
)
@cache_control(600)
def service(
    fmt: SupportedFormatsNoGeoJSON,
    station: List[str] = Query(
        ...,
        description=(
            "Full MOS station identifiers. Please provide the four character "
            "ICAO identifier(s) in the case of airports.  This is done as "
            "some three character identifiers are ambiguous.  You can provide "
            "up to six stations in a single request."
        ),
        max_length=6,
    ),
    model: str = Query(
        ...,
        pattern="^(AVN|GFS|ETA|NAM|NBS|NBE|ECM|LAV|MEX)$",
        max_length=3,
        description="MOS Model ID",
    ),
    runtime: datetime = Query(
        default=None,
        description="MOS Model Cycle Time in UTC please.",
    ),
):
    """Replaced above with module __doc__"""
    if runtime is not None and runtime.tzinfo is None:
        runtime = runtime.replace(tzinfo=timezone.utc)
    # Ensure that provided stations are uppercase and reasonable size
    for stid in station:
        if len(stid) < 3 or len(stid) > 6 or stid.upper() != stid:
            raise HTTPException(
                422,
                detail=(
                    f"Provided station identifier {stid} is invalid, must be "
                    "between 3 and 6 characters in length and uppercase."
                ),
            )

    return Response(
        handler(station, model, runtime, fmt), media_type=MEDIATYPES[fmt]
    )


# Not really used
service.__doc__ = __doc__
