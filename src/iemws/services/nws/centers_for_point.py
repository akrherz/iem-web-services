"""NWS Centers for Point.

Service provides the NWS centers with areal coverage for the provided latitude
and longitude point.  Sadly, this is not an exact science when things like
marine and fire weather zones are included.
"""

import pandas as pd
from fastapi import APIRouter, Query
from sqlalchemy import text

# Local
from ...models import SupportedFormatsNoGeoJSON
from ...models.nws.centers_for_point import Schema
from ...util import deliver_df, get_sqlalchemy_conn

router = APIRouter()


def handler(lon, lat):
    """Handle the request, return dict"""
    with get_sqlalchemy_conn("postgis") as pgconn:
        df = pd.read_sql(
            text(
                """
            with mywfo as (
                select wfo from cwa where
                ST_Contains(the_geom, ST_SetSRID(ST_Point(:lon, :lat), 4326))
                LIMIT 1),
            myrfc as (
                select site_id from rfc where
                ST_Contains(geom, ST_SetSRID(ST_Point(:lon, :lat), 4326))
                LIMIT 1),
            mycwsu as (
                select id as cwsu from cwsu where
                ST_Contains(geom, ST_SetSRID(ST_Point(:lon, :lat), 4326))
                LIMIT 1)
            select wfo, site_id as rfc, cwsu from mywfo, myrfc, mycwsu
            """
            ),
            pgconn,
            params={
                "lat": lat,
                "lon": lon,
            },
            index_col=None,
        )
    return df


@router.get(
    "/nws/centers_for_point.{fmt}",
    description=__doc__,
    tags=[
        "nws",
    ],
    response_model=Schema,
)
def service(
    fmt: SupportedFormatsNoGeoJSON,
    lon: float = Query(..., description="Longitude (deg E)"),
    lat: float = Query(..., description="Latitude (deg N)"),
):
    """Replaced above."""
    df = handler(lon, lat)
    return deliver_df(df, fmt)


service.__doc__ = __doc__
