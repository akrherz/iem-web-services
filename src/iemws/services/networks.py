"""IEM Networks Overview.

For better or worse, the IEM organizes station data into groups called
"networks".  These networks are often delineate political bounds and station
types.
"""

from fastapi import APIRouter
from geopandas import read_postgis

from ..models import SupportedFormats
from ..util import cache_control, deliver_df, get_sqlalchemy_conn

router = APIRouter()


def handler():
    """Handle the request, return dict"""
    with get_sqlalchemy_conn("mesosite") as pgconn:
        df = read_postgis(
            "SELECT *, extent as geom from networks ORDER by id ASC",
            pgconn,
            geom_col="geom",
            index_col=None,
        )
    return df


@router.get(
    "/networks.{fmt}",
    description=__doc__,
    tags=[
        "iem",
    ],
)
@cache_control(3600)
def networks_service(
    fmt: SupportedFormats,
):
    """Replaced above."""
    return deliver_df(handler(), fmt)


networks_service.__doc__ = __doc__
