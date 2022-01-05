"""Provide most recent IEM processed SHEF variables for given station.

This service returns the most recent SHEF processed variables for a given
station.  There is no differentiation here between the COOP and DCP sites,
whatever is available for a given station is returned.  The processing
is generally in "real-time", so everything returned should be current up until
the request time.
"""

from fastapi import Query, APIRouter
from pandas.io.sql import read_sql
from ..util import deliver_df, get_dbconn
from ..models import SupportedFormatsNoGeoJSON
from ..models.last_shef import Schema

router = APIRouter()


def handler(station):
    """Handle the request, return dict"""
    pgconn = get_dbconn("iem")
    df = read_sql(
        "select station, to_char(valid at time zone 'UTC', "
        "'YYYY-MM-DDThh24:MI:SSZ') as utc_valid, "
        "physical_code, duration, source, type, extremum, probability, "
        "depth, dv_interval, depth, qualifier, unit_convention, "
        "product_id, value from current_shef where station = %s "
        "ORDER by physical_code ASC",
        pgconn,
        params=(station,),
        index_col=None,
    )
    return df


@router.get(
    "/last_shef.{fmt}",
    response_model=Schema,
    description=__doc__,
    tags=[
        "nws",
    ],
)
def shef_currents_service(
    fmt: SupportedFormatsNoGeoJSON,
    station: str = Query(..., max_length=8),
):
    """Replaced above with __doc__."""

    return deliver_df(handler(station), fmt)


shef_currents_service.__doc__ = __doc__
