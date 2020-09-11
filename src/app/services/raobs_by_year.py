"""List of Sounding Parameters by Year.

This service provides IEM computed sounding parameters for a given site
and year."""
import datetime

from fastapi import Query, Response
from pandas.io.sql import read_sql
from pyiem.network import Table as NetworkTable
from pyiem.util import utc
from ..reference import MEDIATYPES
from ..util import get_dbconn


def handler(station, year):
    """Handle the request, return dict"""
    pgconn = get_dbconn("postgis")
    stations = [station]
    if station.startswith("_"):
        nt = NetworkTable("RAOB", only_online=False)
        stations = nt.sts[station]["name"].split("--")[1].strip().split()

    sts = utc(year, 1, 1)
    ets = utc(year + 1, 1, 1)

    df = read_sql(
        "SELECT * from raob_flights where station in %s "
        "and valid >= %s and valid < %s ORDER by valid ASC",
        pgconn,
        params=(tuple(stations), sts, ets),
    )

    return df.to_json(orient="table", default_handler=str)


def factory(app):
    """Generate."""

    @app.get("/raobs_by_year.json", description=__doc__)
    def nwstext_service(
        station: str = Query(..., max_length=4, min_length=4),
        year: int = Query(..., min=1947, max=datetime.date.today().year),
    ):
        """Replaced above by __doc__."""
        return Response(handler(station, year), media_type=MEDIATYPES["json"])

    nwstext_service.__doc__ = __doc__
