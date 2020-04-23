"""Provide SHEF Currents for a given pe and duration."""
import os
import tempfile
import warnings

from fastapi import Response, Query
from pandas.io.sql import read_sql
from geopandas import read_postgis
from pyiem.util import get_dbconn

# prevent warnings that may trip up mod_wsgi
warnings.simplefilter("ignore")

CACHE_EXPIRE = 60
# Avoid three table aggregate by initial window join


def get_mckey(fields):
    """What's the key for this request"""
    return "%s_%s_%s" % (
        fields.get("pe", ""),
        fields.get("duration", ""),
        fields.get("days", "2"),
    )


def handler(fmt, pe, duration, days):
    """Handle the request, return dict"""
    pgconn = get_dbconn("iem")
    sql = """
    WITH data as (
        SELECT c.station, c.valid, c.value,
        ST_x(geom) as lon, ST_Y(geom) as lat, geom,
        row_number() OVER (PARTITION by c.station) from
        current_shef c JOIN stations s on (c.station = s.id)
        WHERE physical_code = '%s' and duration = '%s' and
        valid >= now() - '%s days'::interval and value > -9999
    )
    SELECT station,
    to_char(valid at time zone 'UTC', 'YYYY-MM-DDThh24:MI:SSZ') as utc_valid,
    value, lon, lat, geom from data
    where row_number = 1
    """ % (
        pe,
        duration,
        days,
    )

    if fmt == "geojson":
        df = read_postgis(sql, pgconn, geom_col="geom")
    else:
        df = read_sql(sql, pgconn)
        df.drop("geom", axis=1, inplace=True)
    if fmt == "txt":
        return df.to_csv(index=False)
    if fmt == "json":
        # Implement our 'table-schema' option
        return df.to_json(orient="table", default_handler=str)
    if df.empty:
        return {"type": "FeatureCollection", "features": []}
    (tmpfd, tmpfn) = tempfile.mkstemp(text=True)
    df.to_file(tmpfn, driver="GeoJSON")

    res = open(tmpfn).read()
    os.unlink(tmpfn)
    return res


def factory(app):
    """Generate."""

    @app.get("/shef_currents.{fmt}", description=__doc__)
    def shef_currents_service(
        fmt: str = Query(...),
        pe: str = Query(..., max_length=2),
        duration: str = Query(..., max_length=1),
        days: int = Query(1),
    ):
        """Babysteps."""
        mediatypes = {
            "json": "application/json",
            "geojson": "application/vnd.geo+json",
            "txt": "text/plain",
        }
        return Response(
            handler(fmt, pe, duration, days), media_type=mediatypes[fmt]
        )

    shef_currents_service.__doc__ = __doc__
