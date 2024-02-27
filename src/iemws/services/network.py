"""IEM Station Metadata for One Network.

The IEM organizes stations into networks.  This service returns station
metadata for a given network.
"""

from fastapi import APIRouter, HTTPException, Path
from geopandas import read_postgis

from ..models import SupportedFormats
from ..util import cache_control, deliver_df, get_sqlalchemy_conn

router = APIRouter()


def handler(network_id):
    """Handle the request, return dict"""
    with get_sqlalchemy_conn("mesosite") as pgconn:
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
    # Hackaround downstream issues
    df["archive_begin"] = df["archive_begin"].astype("datetime64[ns]")
    df["archive_end"] = df["archive_end"].astype("datetime64[ns]")
    return df


@router.get(
    "/network/{network_id}.{fmt}",
    description=__doc__,
    tags=[
        "iem",
    ],
)
@cache_control(600)
def service(
    fmt: SupportedFormats,
    network_id: str = Path(..., description="IEM Network Identifier."),
):
    """Replaced above."""
    df = handler(network_id)
    return deliver_df(df, fmt)


service.__doc__ = __doc__
