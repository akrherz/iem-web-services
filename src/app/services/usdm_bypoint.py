"""US Drought Monitor by lat/lon point"""
import datetime
import json

from pandas.io.sql import read_sql
from pyiem.util import get_dbconn

ISO = "%Y-%m-%dT%H:%M:%SZ"


def get_mckey(fields):
    """What's the key for this request"""
    sdate = fields.get("sdate", "")
    edate = fields.get("edate", "")
    lat = float(fields.get("lat", 42.0))
    lon = float(fields.get("lon", -95.0))
    return "%s/%s/%s/%s" % (sdate, edate, lon, lat)


def run(sdate, edate, lon, lat):
    """Do the work, please"""
    pgconn = get_dbconn("postgis")

    df = read_sql(
        """
        SELECT to_char(valid, 'YYYY-MM-DD') as valid,
        max(dm) as category from usdm WHERE
        ST_Contains(geom, ST_SetSRID(ST_GeomFromEWKT('POINT(%s %s)'),4326))
        and valid >= %s and valid <= %s
        GROUP by valid ORDER by valid ASC
    """,
        pgconn,
        params=(lon, lat, sdate, edate),
        index_col=None,
    )

    return json.loads(df.to_json(orient="table", default_handler=str))


def handler(sdate, edate, lon, lat):
    """Handle the request, return dict"""

    return run(sdate, edate, lon, lat)
