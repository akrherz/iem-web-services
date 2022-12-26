"""NWS UGCS (Zones/Counties) Metadata.

The National Weather Service often issues products specific to counties or
forecast zones.  They use Universal Geographic Codes to represent these areas.
These UGC codes are six characters.

The NWS UGC database has changed over time as new forecast zones are defined
or removed.  Sometimes counties / forecast zones are reassigned to a different
NWS Weather Forecast Office.  So this service takes an optional timestamp flag
to provide an archived version of this database.

The IEM has attempted to properly keep track of the UGC database since 2007.

For GeoJSON, this service returns simplified geometries as the full resolution
dataset is very large.
"""
from datetime import datetime, timezone

from geopandas import read_postgis
from fastapi import Query, APIRouter
from sqlalchemy import text
from pyiem.util import utc
from ...models import SupportedFormats
from ...models.nws.ugcs import UGCSchema
from ...util import get_dbconn, deliver_df

router = APIRouter()


def handler(state, wfo, valid, just_firewx):
    """Handle the request, return dict"""
    params = {
        "state": state,
        "wfo": wfo,
        "valid": valid,
    }
    pgconn = get_dbconn("postgis")
    state_limiter = ""
    if state is not None:
        state_limiter = " and substr(ugc, 1, 2) = :state "
    wfo_limiter = ""
    if wfo is not None:
        wfo_limiter = " and wfo = :wfo "
    firewx = " and source != 'fz' "
    if just_firewx:
        firewx = " and source = 'fz' "
    df = read_postgis(
        text(
            f"""
        SELECT ugc, simple_geom as geom, name, state, wfo from ugcs WHERE
        ((begin_ts <= :valid and end_ts > :valid) or
        (begin_ts <= :valid and end_ts is null)) {wfo_limiter} {state_limiter}
        {firewx} ORDER by ugc ASC
        """
        ),
        pgconn,
        geom_col="geom",
        params=params,
        index_col=None,
    )
    return df


@router.get(
    "/nws/ugcs.{fmt}",
    response_model=UGCSchema,
    description=__doc__,
    tags=[
        "nws",
    ],
)
def service(
    fmt: SupportedFormats,
    state: str = Query(None, min_length=2, max_length=2),
    wfo: str = Query(None, min_length=3, max_length=3),
    valid: datetime = Query(None),
    just_firewx: bool = Query(
        False, description="Just include Fire Weather Zones"
    ),
):
    """Replaced above."""
    if valid is not None:
        valid = valid.replace(tzinfo=timezone.utc)
    else:
        valid = utc()
    df = handler(state, wfo, valid, just_firewx)
    return deliver_df(df, fmt)


service.__doc__ = __doc__
