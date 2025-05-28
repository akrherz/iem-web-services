"""NWS Local Storm Reports (LSR)s by point.

This service emits NWS Local Storm Reports (LSR)s for a given
latitude/longitude and over an optional period of time.  You can specify a
search radius as either `radius_degrees` (value less than 10) or
`radius_miles` (value less than 1000).  Since LSRs are
provided with only 0.01 degree lat/lon precision, don't get excited that this
radius search is an exact science!

This service has data back to 2002 or so, but data quality isn't the greatest
prior to 2005/2006.
"""

from datetime import datetime
from zoneinfo import ZoneInfo

import geopandas as gpd
from fastapi import APIRouter, Query
from metpy.units import units
from pyiem.database import sql_helper

# Local
from ...models import SupportedFormats
from ...models.nws.lsrs_by_point import Schema
from ...util import deliver_df, get_sqlalchemy_conn

router = APIRouter()


def handler(lon, lat, radius_degrees, radius_miles, begints, endts):
    """Handle the request, return dict"""
    params = {
        "lon": lon,
        "lat": lat,
        "begints": begints,
        "endts": endts,
        "radius_degrees": radius_degrees,
    }
    # Figure out which radius search to use
    if radius_degrees is not None:
        searchgeo = "ST_SetSrid(ST_MakePoint(:lon, :lat), 4326)"
        spatial = f"ST_DWithin(l.geom, {searchgeo}, :radius_degrees)"
    else:
        params["meters"] = (units("mile") * radius_miles).to(units("m")).m
        searchgeo = "ST_MakePoint(:lon, :lat)::geography"
        spatial = f"ST_DWithin(l.geom::geography, {searchgeo}, :meters)"
    temporal = ""
    if begints is not None and endts is not None:
        params["begints"] = params["begints"].replace(tzinfo=ZoneInfo("UTC"))
        params["endts"] = params["endts"].replace(tzinfo=ZoneInfo("UTC"))
        temporal = " and valid >= :begints and valid < :endts "
    with get_sqlalchemy_conn("postgis") as pgconn:
        df = gpd.read_postgis(
            sql_helper(
                """
            select to_char(valid at time zone 'UTC', 'YYYY-MM-DDThh24:MI:SSZ')
            as valid, type, magnitude, city, county, l.state, l.source, remark,
            l.wfo, typetext, l.geom, product_id, unit, qualifier, ugc,
            product_id_summary, ST_x(l.geom) as lon, ST_y(l.geom) as lat
            from lsrs l LEFT JOIN ugcs u on (l.gid = u.gid)
            WHERE {spatial} {temporal}
            """,
                spatial=spatial,
                temporal=temporal,
            ),
            pgconn,
            geom_col="geom",
            params=params,
            index_col=None,
        )  # type: ignore
    return df


@router.get(
    "/nws/lsrs_by_point.{fmt}",
    description=__doc__,
    tags=[
        "nws",
    ],
    response_model=Schema,
)
def service(
    fmt: SupportedFormats,
    lon: float = Query(..., description="Longitude (deg E)"),
    lat: float = Query(..., description="Latitude (deg N)"),
    radius_degrees: float = Query(
        None,
        description="Search radius in decimal degrees.",
        gt=0,
        lt=10,
    ),
    radius_miles: float = Query(
        None,
        description="Search radius in decimal miles.",
        gt=0,
        lt=1000,
    ),
    begints: datetime = Query(
        None, description="UTC Inclusive Timestamp to start search for LSRs."
    ),
    endts: datetime = Query(
        None, description="UTC Timestamp to end search for LSRs."
    ),
):
    """Replaced above."""
    if radius_degrees is None and radius_miles is None:
        radius_degrees = 1
    df = handler(lon, lat, radius_degrees, radius_miles, begints, endts)
    return deliver_df(df, fmt)


service.__doc__ = __doc__
