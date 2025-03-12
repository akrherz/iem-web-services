"""List of Sounding Station Parameters.

This service provides IEM computed sounding parameters for a given station
and optional year.  If you do not specify a year, the service will only
work for CSV output as the JSON is too large!"""

from datetime import date
from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from pyiem.database import sql_helper
from pyiem.network import Table as NetworkTable
from pyiem.util import utc

from ..models import SupportedFormatsNoGeoJSON
from ..util import deliver_df, get_sqlalchemy_conn

router = APIRouter()


def handler(station: str, year: Optional[int]) -> pd.DataFrame:
    """Handle the request, return dict"""
    params = {
        "station": station,
        "sts": None,
        "ets": None,
    }
    station_limiter = "station = :station"
    if station.startswith("_"):
        station_limiter = "station = ANY(:station)"
        nt = NetworkTable("RAOB", only_online=False)
        params["station"] = (
            nt.sts[station]["name"].split("--")[1].strip().split()
        )

    time_limiter = ""
    if year is not None:
        time_limiter = " and valid >= :sts and valid < :ets"
        params["sts"] = utc(year, 1, 1)
        params["ets"] = utc(year + 1, 1, 1)
    with get_sqlalchemy_conn("raob") as pgconn:
        df = pd.read_sql(
            sql_helper(
                """
    SELECT * from raob_flights where {station_limiter}
    {time_limiter} ORDER by valid ASC
    """,
                station_limiter=station_limiter,
                time_limiter=time_limiter,
            ),
            pgconn,
            params=params,
        )

    return df


@router.get(
    "/raobs_by_year.{fmt}",
    description=__doc__,
    tags=[
        "nws",
    ],
)
def nwstext_service(
    fmt: SupportedFormatsNoGeoJSON,
    station: str = Query(..., max_length=4, min_length=4),
    year: Optional[int] = Query(None, ge=1947, le=date.today().year),
):
    """Replaced above by __doc__."""
    if fmt == "json" and year is None:
        raise HTTPException(status_code=422, detail="JSON requires a year set")
    return deliver_df(handler(station, year), fmt)


nwstext_service.__doc__ = __doc__
