"""US Drought Monitor by lat/lon point"""
import json
from datetime import date

from pandas.io.sql import read_sql
from fastapi import Query
from ..util import get_dbconn

ISO = "%Y-%m-%dT%H:%M:%SZ"


def run(sdate, edate, lon, lat):
    """Do the work, please"""
    pgconn = get_dbconn("postgis")

    df = read_sql(
        "SELECT to_char(valid, 'YYYY-MM-DD') as valid, "
        "max(dm) as category from usdm WHERE "
        "ST_Contains(geom, ST_SetSRID(ST_GeomFromEWKT('POINT(%s %s)'),4326)) "
        "and valid >= %s and valid <= %s GROUP by valid ORDER by valid ASC",
        pgconn,
        params=(lon, lat, sdate, edate),
        index_col=None,
    )

    return json.loads(df.to_json(orient="table", default_handler=str))


def handler(sdate, edate, lon, lat):
    """Handle the request, return dict"""

    return run(sdate, edate, lon, lat)


def factory(app):
    """Generate."""

    @app.get("/usdm_bypoint.json", description=__doc__)
    def usdm_bypoint_service(
        sdate: date = Query(...),
        edate: date = Query(...),
        lon: float = Query(...),
        lat: float = Query(...),
    ):
        """Replaced above."""
        return handler(sdate, edate, lon, lat)

    usdm_bypoint_service.__doc__ = __doc__
