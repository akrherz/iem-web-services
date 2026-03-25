"""Provide SHEF Currents for a given pe and duration."""

from datetime import timedelta
from typing import Annotated

from fastapi import APIRouter, Query
from geopandas import read_postgis
from pyiem.database import sql_helper

from ..models import SupportedFormats
from ..util import deliver_df, get_sqlalchemy_conn

router = APIRouter()


def handler(pe, duration, days):
    """Handle the request, return dict"""
    sql = sql_helper("""
    WITH data as (
        SELECT c.station, c.valid, c.value,
        ST_x(geom) as lon, ST_Y(geom) as lat, geom,
        row_number() OVER (PARTITION by c.station) from
        current_shef c JOIN stations s on (c.station = s.id)
        WHERE physical_code = :pe and duration = :duration and
        valid >= now() - :delta and value > -9999
    )
    SELECT station,
    to_char(valid at time zone 'UTC', 'YYYY-MM-DDThh24:MI:SSZ') as utc_valid,
    value, lon, lat, geom from data
    where row_number = 1
    """)
    params = {
        "pe": pe,
        "duration": duration,
        "delta": timedelta(days=1),
    }
    with get_sqlalchemy_conn("iem") as pgconn:
        df = read_postgis(sql, pgconn, params=params, geom_col="geom")  # type: ignore
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
    pe: Annotated[
        str,
        Query(
            max_length=2, description="SHEF 2 character physical elemenet code"
        ),
    ],
    duration: Annotated[
        str, Query(max_length=1, description="SHEF 1 character duration code")
    ],
    days: Annotated[
        int,
        Query(
            ge=1, le=3000, description="Number of days to look back for data"
        ),
    ] = 1,
):
    """Replaced above with __doc__."""

    return deliver_df(handler(pe, duration, days), fmt)


shef_currents_service.__doc__ = __doc__
