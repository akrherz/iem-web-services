"""NWS Storm Based Warnings by Line (and Time)

This service returns storm based warnings that spatially and temporally
overlap with a simple line geometry.  The line is assumed to be in EPSG:4326
and cartesian.  By default, only the issuance polygons are returned, but
you can request them all via the `include_svs` parameter.
"""

from datetime import datetime, timezone

import geopandas as gpd
from fastapi import APIRouter, Query
from sqlalchemy import text

# Local
from ...models import SupportedFormats
from ...models.nws.sbw import StormBasedWarningSchema
from ...util import deliver_df, get_sqlalchemy_conn

router = APIRouter()


def handler(
    start_lat: float,
    start_lon: float,
    end_lat: float,
    end_lon: float,
    sts: datetime,
    ets: datetime,
    include_svs: bool,
):
    """Handle the request, return dict"""
    issuecol = "issue"
    expirecol = "expire"
    status_filter = " and status = 'NEW' "
    if include_svs:
        issuecol = "polygon_begin"
        expirecol = "polygon_end"
        status_filter = ""
    with get_sqlalchemy_conn("postgis") as pgconn:
        df = gpd.read_postgis(
            text(
                f"""
            select wfo, geom, vtec_year, phenomena, significance, eventid,
            windtag, hailtag, tornadotag, damagetag, is_emergency, is_pds,
            windthreat, hailthreat, squalltag, product_id, product_signature,
            '' as uri
            from sbw where
            ST_Intersects(
                ST_MakeLine(
                    ST_Point(:start_lon, :start_lat, 4326),
                    ST_Point(:end_lon, :end_lat, 4326)), geom)
            and {issuecol} <= :ets and {expirecol} >= :sts {status_filter}
            """
            ),
            pgconn,
            params={
                "start_lat": start_lat,
                "start_lon": start_lon,
                "end_lat": end_lat,
                "end_lon": end_lon,
                "sts": sts,
                "ets": ets,
            },
            geom_col="geom",
            index_col=None,
        )
    # NOTE the above has duplicated entries, so we 'dedup'
    if not df.empty:
        df["uri"] = (
            "https://mesonet.agron.iastate.edu/vtec/#"
            + df["vtec_year"].astype(str)
            + "-O-NEW-K"
            + df["wfo"]
            + "-"
            + df["phenomena"]
            + "-"
            + df["significance"]
            + "-"
            + df["eventid"].astype(str).str.pad(4, fillchar="0")
        )
    return df


@router.get(
    "/nws/sbw_by_line.{fmt}",
    description=__doc__,
    tags=[
        "vtec",
    ],
    response_model=StormBasedWarningSchema,
)
def service(
    fmt: SupportedFormats,
    start_lat: float = Query(
        ..., description="Starting Latitude (deg N)", ge=-90, le=90
    ),
    start_lon: float = Query(
        ..., description="Starting Longitude (deg E)", ge=-180, le=180
    ),
    end_lat: float = Query(
        ..., description="Ending Latitude (deg N)", ge=-90, le=90
    ),
    end_lon: float = Query(
        ..., description="Ending Longitude (deg E)", ge=-180, le=180
    ),
    begints: datetime = Query(
        ...,
        description="Inclusive UTC Start of the time period to query for",
    ),
    endts: datetime = Query(
        ...,
        description="Inclusive UTC End of the time period to query for",
    ),
    include_svs: bool = Query(
        False,
        description="Include Severe Weather Statements Polygons in the output",
    ),
):
    """Replaced above."""
    sts = begints.replace(tzinfo=timezone.utc)
    ets = endts.replace(tzinfo=timezone.utc)
    df = handler(start_lat, start_lon, end_lat, end_lon, sts, ets, include_svs)
    return deliver_df(df, fmt)


service.__doc__ = __doc__
