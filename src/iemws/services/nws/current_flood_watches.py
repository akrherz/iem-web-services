"""NWS Current **Point** Flood Watches.

This service provides a current listing of NWS Flood Watches for forecast
points.  These are watches that contain a HVTEC NWSLI, which is the forecast
point the NWS uses.  There is no archive support to this app, it is what
drives the data presentation on
[IEM Rivers](https://mesonet.agron.iastate.edu/rivers/).

This service only provides the watches for points that the NWS publishes
metadata for [here](https://www.weather.gov/vtec/Valid-Time-Event-Code).

The forecast watch point is included as an attribute ``latitude`` and
``longitude``, the actual geometries here are the polygons associated with
the watches.  The data returned is sorted by river name and then crudely by
forecast watch point latitude descending (north to south).
"""

from fastapi import APIRouter, Query

from ...models import SupportedFormats
from ...util import deliver_df
from .current_flood_warnings import handler

router = APIRouter()


@router.get(
    "/nws/current_flood_watches.{fmt}",
    description=__doc__,
    tags=[
        "vtec",
    ],
)
def service(
    fmt: SupportedFormats,
    state: str = Query(None, min_length=2, max_length=2),
    wfo: str = Query(None, min_length=3, max_length=3),
):
    """Replaced above."""
    df = handler(state, wfo, "A")
    return deliver_df(df, fmt)


service.__doc__ = __doc__
