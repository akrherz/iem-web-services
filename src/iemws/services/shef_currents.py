"""Provide SHEF Currents for a given pe and duration."""
import tempfile

from fastapi import Response, Query, APIRouter
from pandas.io.sql import read_sql
from geopandas import read_postgis
from ..util import get_dbconn
from ..models import SupportedFormats
from ..reference import MEDIATYPES

router = APIRouter()


def handler(fmt, pe, duration, days):
    """Handle the request, return dict"""
    pgconn = get_dbconn("iem")
    sql = f"""
    WITH data as (
        SELECT c.station, c.valid, c.value,
        ST_x(geom) as lon, ST_Y(geom) as lat, geom,
        row_number() OVER (PARTITION by c.station) from
        current_shef c JOIN stations s on (c.station = s.id)
        WHERE physical_code = '{pe}' and duration = '{duration}' and
        valid >= now() - '{days} days'::interval and value > -9999
    )
    SELECT station,
    to_char(valid at time zone 'UTC', 'YYYY-MM-DDThh24:MI:SSZ') as utc_valid,
    value, lon, lat, geom from data
    where row_number = 1
    """

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
    with tempfile.NamedTemporaryFile("w", delete=True) as tmp:
        df.to_file(tmp.name, driver="GeoJSON")
        res = open(tmp.name).read()
    return res


@router.get("/shef_currents.{fmt}", description=__doc__)
def shef_currents_service(
    fmt: SupportedFormats,
    pe: str = Query(..., max_length=2),
    duration: str = Query(..., max_length=1),
    days: int = Query(1),
):
    """Replaced above with __doc__."""

    return Response(
        handler(fmt, pe, duration, days), media_type=MEDIATYPES[fmt]
    )


shef_currents_service.__doc__ = __doc__
