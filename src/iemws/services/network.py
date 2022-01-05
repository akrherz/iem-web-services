"""IEM Station Metadata for One Network.

The IEM organizes stations into networks.  This service returns station
metadata for a given network.
"""

from geopandas import read_postgis
from fastapi import Query, HTTPException, APIRouter
from ..models import SupportedFormats
from ..util import get_dbconn, deliver_df

router = APIRouter()


def handler(network_id):
    """Handle the request, return dict"""
    pgconn = get_dbconn("mesosite")

    # One off
    if network_id == "ASOS1MIN":
        df = read_postgis(
            "SELECT t.*, ST_X(geom) as longitude, ST_Y(geom) as latitude "
            "from stations t JOIN station_attributes a "
            "ON (t.iemid = a.iemid) WHERE t.network ~* 'ASOS' and "
            "a.attr = 'HAS1MIN' ORDER by id ASC",
            pgconn,
            geom_col="geom",
            index_col=None,
        )
    else:
        df = read_postgis(
            "SELECT *, ST_X(geom) as longitude, ST_Y(geom) as latitude "
            "from stations where network = %s ORDER by name ASC",
            pgconn,
            params=(network_id,),
            geom_col="geom",
            index_col=None,
        )
    if df.empty:
        raise HTTPException(
            status_code=404,
            detail="No stations found for provided network.",
        )
    return df


@router.get(
    "/network/{network_id}.{fmt}",
    description=__doc__,
    tags=[
        "iem",
    ],
)
def usdm_bypoint_service(
    fmt: SupportedFormats,
    network_id: str = Query(..., description="IEM Network Identifier."),
):
    """Replaced above."""
    df = handler(network_id)
    return deliver_df(df, fmt)


usdm_bypoint_service.__doc__ = __doc__
