"""NWS Current **Point** Flood Warnings.

This service provides a current listing of NWS Flood Warnings for forecast
points.  These are warnings that contain a HVTEC NWSLI, which is the forecast
point the NWS uses.  There is no archive support to this app, it is what
drives the data presentation on
[IEM Rivers](https://mesonet.agron.iastate.edu/rivers/).

This service only provides the warnings for points that the NWS publishes
metadata for [here](https://www.weather.gov/vtec/Valid-Time-Event-Code).

The forecast warning point is included as an attribute ``latitude`` and
``longitude``, the actual geometries here are the polygons associated with
the warnings.  The data returned is sorted by river name and then crudely by
forecast warning point latitude descending (north to south).
"""

import geopandas as gpd
from fastapi import APIRouter, Query
from pyiem.database import sql_helper

from ...models import SupportedFormats
from ...util import deliver_df, get_sqlalchemy_conn

router = APIRouter()


def handler(state, wfo, significance: str) -> gpd.GeoDataFrame:
    """Handle the request, return dict"""
    params = {
        "significance": significance,
        "state": state,
        "wfo": wfo,
    }
    state_limiter = ""
    if state is not None:
        state_limiter = " and substr(w.ugc, 1, 2) = :state "
    wfo_limiter = ""
    if wfo is not None:
        wfo_limiter = " and w.wfo = :wfo "
    with get_sqlalchemy_conn("postgis") as pgconn:
        df = gpd.read_postgis(
            sql_helper(
                """
            WITH polys as (
                SELECT wfo, eventid, hvtec_nwsli, w.geom, h.name, h.river_name,
                st_x(h.geom) as longitude, st_y(h.geom) as latitude,
                phenomena, significance
                from sbw w JOIN hvtec_nwsli h on (w.hvtec_nwsli = h.nwsli)
                where expire > now() and phenomena = 'FL' and
                significance = :significance and
                polygon_end > now() and status not in ('EXP', 'CAN') and
                hvtec_nwsli is not null {wfo_limiter}),
            counties as (
                SELECT w.hvtec_nwsli, sumtxt(u.name || ', ') as counties from
                warnings w JOIN ugcs u on (w.gid = u.gid) WHERE
                w.expire > now() and phenomena = 'FL'
                and significance = :significance
                and status NOT IN ('EXP','CAN') {wfo_limiter} {state_limiter}
                GROUP by hvtec_nwsli),
            agg as (
                SELECT p.*, c.counties
                from polys p JOIN counties c on (p.hvtec_nwsli = c.hvtec_nwsli)
            )
            SELECT r.*, a.* from riverpro r JOIN agg a on
                (r.nwsli = a.hvtec_nwsli)
            ORDER by a.river_name ASC, latitude DESC
            """,
                wfo_limiter=wfo_limiter,
                state_limiter=state_limiter,
            ),
            pgconn,
            geom_col="geom",
            index_col=None,
            params=params,
        )  # type: ignore
    return df


@router.get(
    "/nws/current_flood_warnings.{fmt}",
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
    df = handler(state, wfo, "W")
    return deliver_df(df, fmt)


service.__doc__ = __doc__
