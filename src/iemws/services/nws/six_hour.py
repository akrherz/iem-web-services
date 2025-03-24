"""NWS Six Hour Precip/Snowfall/SnowDepth/Snow Water Equiv Reports.

Via mostly paid and some volunteer reports, the NWS collects six hour
snowfall totals from a very limited number of locations.  These observations
are made at 00, 06, 12, and 18 UTC.  This service provides access to these
reports as collected via disseminated SHEF products.

Trace values are encoded as ``0.0001``.
"""

from datetime import datetime
from enum import Enum

import geopandas as gpd
from fastapi import APIRouter, Query
from pyiem.database import sql_helper

from ...models import SupportedFormats
from ...models.nws.six_hour import Schema
from ...util import deliver_df, get_sqlalchemy_conn

router = APIRouter()
LOOKUP = {
    "snowfall": "SFQRZZZ",
    "precip": "PPQRZZZ",
    "snowdepth": "SDIRZZZ",
    "swe": "SWIRZZZ",
}


def handler(valid: datetime, varname: str) -> gpd.GeoDataFrame:
    """Handle the request, return dict"""
    params = {
        "valid": valid,
        "shefvar": LOOKUP[varname],
    }
    with get_sqlalchemy_conn("hads") as pgconn:
        df = gpd.read_postgis(
            sql_helper(
                """
            select station, network, key as shefvar, value, geom, wfo,
            ugc_county, valid at time zone 'UTC' as utc_valid,
            st_x(geom) as longitude, st_y(geom) as latitude, name, state from
            raw r, stations t where valid = :valid and key = :shefvar
            and r.station = t.id and
            (t.network ~* 'COOP' or t.network ~* 'DCP')
            order by station asc, network asc
            """
            ),
            pgconn,
            geom_col="geom",
            params=params,
            index_col=None,
        )  # type: ignore
        # We have duplicate network entries, so we do a hack
        if not df.empty:
            df = df.groupby("station").first()
    return df


@router.get(
    "/nws/{varname}_6hour.{fmt}",
    description=__doc__,
    tags=[
        "nws",
    ],
    response_model=Schema,
)
def service(
    fmt: SupportedFormats,
    varname: str = Enum("varname", ["snowfall", "precip", "snowdepth", "swe"]),
    valid: datetime = Query(
        ..., description="UTC Timestamp to return reports for."
    ),
):
    """Replaced above."""
    df = handler(valid, varname)
    return deliver_df(df, fmt)


service.__doc__ = __doc__
