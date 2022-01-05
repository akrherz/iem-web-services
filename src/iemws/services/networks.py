"""IEM Networks Overview.

For better or worse, the IEM organizes station data into groups called
"networks".  These networks are often delineate political bounds and station
types.  One noticable one-off is the Iowa ASOS/AWOS data.  There is a
dedicated network called ``AWOS`` which represents the airport weather stations
within the state that are not maintained by the NWS+FAA.
"""

from geopandas import read_postgis
from fastapi import APIRouter
from ..models import SupportedFormats
from ..util import get_dbconn, deliver_df

router = APIRouter()


def handler():
    """Handle the request, return dict"""
    pgconn = get_dbconn("mesosite")

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
def networks_service(
    fmt: SupportedFormats,
):
    """Replaced above."""
    return deliver_df(handler(), fmt)


networks_service.__doc__ = __doc__
