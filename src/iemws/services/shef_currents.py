"""Provide SHEF Currents for a given pe and duration."""

from fastapi import APIRouter, Query
from geopandas import read_postgis

from ..models import SupportedFormats
from ..util import deliver_df, get_dbconn

router = APIRouter()


def handler(pe, duration, days):
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
    df = read_postgis(sql, pgconn, geom_col="geom")
    return df


@router.get(
    "/shef_currents.{fmt}",
    description=__doc__,
    tags=[
        "nws",
    ],
)
def shef_currents_service(
    fmt: SupportedFormats,
    pe: str = Query(..., max_length=2),
    duration: str = Query(..., max_length=1),
    days: int = Query(1),
):
    """Replaced above with __doc__."""

    return deliver_df(handler(pe, duration, days), fmt)


shef_currents_service.__doc__ = __doc__
