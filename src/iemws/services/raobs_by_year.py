"""List of Sounding Parameters by Year.

This service provides IEM computed sounding parameters for a given site
and year."""

from datetime import date

from fastapi import APIRouter, Query
from pandas.io.sql import read_sql
from pyiem.network import Table as NetworkTable
from pyiem.util import utc
from sqlalchemy import text

from ..util import deliver_df, get_sqlalchemy_conn

router = APIRouter()


def handler(station, year):
    """Handle the request, return dict"""
    stations = [station]
    if station.startswith("_"):
        nt = NetworkTable("RAOB", only_online=False)
        stations = nt.sts[station]["name"].split("--")[1].strip().split()

    sts = utc(year, 1, 1)
    ets = utc(year + 1, 1, 1)
    with get_sqlalchemy_conn("raob") as pgconn:
        df = read_sql(
            text(
                "SELECT * from raob_flights where station = ANY(:ids) "
                "and valid >= :sts and valid < :ets ORDER by valid ASC"
            ),
            pgconn,
            params={"ids": stations, "sts": sts, "ets": ets},
        )

    return df


@router.get(
    "/raobs_by_year.json",
    description=__doc__,
    tags=[
        "nws",
    ],
)
def nwstext_service(
    station: str = Query(..., max_length=4, min_length=4),
    year: int = Query(..., ge=1947, le=date.today().year),
):
    """Replaced above by __doc__."""
    df = handler(station, year)
    return deliver_df(df, "json")


nwstext_service.__doc__ = __doc__
