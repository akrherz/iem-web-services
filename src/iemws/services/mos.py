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
"""
from datetime import datetime
from typing import List

import pytz
from fastapi import APIRouter, HTTPException, Query, Response
from pandas.io.sql import read_sql
from pyiem import util
from sqlalchemy import text

from ..models import SupportedFormatsNoGeoJSON
from ..reference import MEDIATYPES
from ..util import get_dbconn

MODEL_DOMAIN = ["AVN", "GFS", "ETA", "NAM", "NBS", "NBE", "ECM", "LAV", "MEX"]
COLUMNS = (
    "station,model,runtime,ftime,n_x,tmp,dpt,cld,wdr,wsp,p06,p12,"
    "q06,q12,t06,t12,snw,cig,vis,obv,poz,pos,typ,sky,swh,lcb,"
    "i06,slv,s06,pra,ppl,psn,pzr,t03,gst,q24,p24,t24,ccg,ppo,pco,lp1,lc1,cp1,"
    "cc1,s12,i12,s24,pzp,prs,txn"
).split(",")
router = APIRouter()


def find_runtime(station, model):
    """Figure out what our latest runtime is."""
    cursor = util.get_dbconn("mos").cursor()
    cursor.execute(
        "SELECT max(runtime) from alldata WHERE model = %s and "
        "station = ANY(%s) and runtime > now() - '48 hours'::interval",
        (model, station),
    )
    if cursor.rowcount == 0:
        raise HTTPException(404, detail="could not find most recent model run")
    return cursor.fetchone()[0]


def handler(station, model, runtime, fmt):
    """Handle the request."""
    if model not in MODEL_DOMAIN:
        raise HTTPException(503, detail="model= is not in processed domain")
    if runtime is None:
        runtime = find_runtime(station, model)

    # Ready to get the data!
    df = read_sql(
        text(
            "SELECT *, t06_1 ||'/'||t06_2 as t06, t12_1 ||'/'|| t12_2 as t12, "
            "runtime at time zone 'UTC' as runtime_utc, "
            "ftime at time zone 'UTC' as ftime_utc "
            "from alldata where model = :model and station = ANY(:ids) and "
            "runtime = :runtime ORDER by station ASC, ftime ASC"
        ),
        get_dbconn("mos"),
        params={"model": model, "ids": station, "runtime": runtime},
        index_col=None,
    )
    if df.empty:
        raise HTTPException(404, "No data found for query.")
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
def service(
    fmt: SupportedFormatsNoGeoJSON,
    station: List[str] = Query(
        ..., description="Full MOS Station Identifier", max_length=6
    ),
    model: str = Query(..., description="MOS Model ID"),
    runtime: datetime = Query(None, description="MOS Model Cycle Time"),
):
    """Replaced above with module __doc__"""
    if runtime is not None and runtime.tzinfo is None:
        runtime = runtime.replace(tzinfo=pytz.UTC)

    return Response(
        handler(station, model, runtime, fmt), media_type=MEDIATYPES[fmt]
    )


# Not really used
service.__doc__ = __doc__
