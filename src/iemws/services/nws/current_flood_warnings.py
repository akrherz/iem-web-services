"""NWS Current **Point** Flood Warnings.

This service provides a current listing of NWS Flood Warnings for forecast
points.  These are warnings that contain a HVTEC NWSLI, which is the forecast
point the NWS uses.  There is no archive support to this app, it is what
drives the data presentation on
[IEM Rivers](https://mesonet.agron.iastate.edu/rivers).

This service only provides the warnings for points that the NWS publishes
metadata for [here](https://www.weather.gov/vtec/Valid-Time-Event-Code).

The forecast warning point is included as an attribute ``latitude`` and
``longitude``, the actual geometries here are the polygons associated with
the warnings.
"""
import tempfile

import pandas as pd
from geopandas import read_postgis
from fastapi import Response, Query
from ...models import SupportedFormats
from ...reference import MEDIATYPES
from ...util import get_dbconn


def handler(fmt, state, wfo):
    """Handle the request, return dict"""
    pgconn = get_dbconn("postgis")
    state_limiter = ""
    if state is not None:
        state_limiter = f" and substr(w.ugc, 1, 2) = '{state}' "
    wfo_limiter = ""
    if wfo is not None:
        wfo_limiter = f" and wfo = '{wfo}' "

    df = read_postgis(
        f"""
        WITH polys as (
            SELECT wfo, eventid, hvtec_nwsli, s.geom, h.name, h.river_name,
            st_x(h.geom) as longitude, st_y(h.geom) as latitude
            from sbw s JOIN hvtec_nwsli h on (s.hvtec_nwsli = h.nwsli)
            where phenomena = 'FL' and significance = 'W' and
            polygon_end > now() and status not in ('EXP', 'CAN') and
            hvtec_nwsli is not null {wfo_limiter}),
        counties as (
            SELECT w.hvtec_nwsli, sumtxt(u.name || ', ') as counties from
            warnings w JOIN ugcs u on (w.gid = u.gid) WHERE
            w.expire > now() and phenomena = 'FL' and significance = 'W'
            and status NOT IN ('EXP','CAN') {wfo_limiter} {state_limiter}
            GROUP by hvtec_nwsli),
        agg as (
            SELECT p.*, c.counties
            from polys p JOIN counties c on (p.hvtec_nwsli = c.hvtec_nwsli)
        )
        SELECT r.*, a.* from riverpro r JOIN agg a on (r.nwsli = a.hvtec_nwsli)
        ORDER by a.river_name ASC
        """,
        pgconn,
        geom_col="geom",
        index_col=None,
    )

    if fmt != "geojson":
        df = df.drop("geom", axis=1)
        df = pd.DataFrame(df)
    if fmt == "txt":
        return df.to_csv(index=False)
    if fmt == "json":
        return df.to_json(orient="table", index=False)
    with tempfile.NamedTemporaryFile("w", delete=True) as tmp:
        if df.empty:
            return """{"type": "FeatureCollection", "features": []}"""

        df.to_file(tmp.name, driver="GeoJSON")
        with open(tmp.name) as fh:
            res = fh.read()
    return res


def factory(app):
    """Generate."""

    @app.get("/nws/current_flood_warnings.{fmt}", description=__doc__)
    def service(
        fmt: SupportedFormats,
        state: str = Query(None, length=2),
        wfo: str = Query(None, length=3),
    ):
        """Replaced above."""
        return Response(handler(fmt, state, wfo), media_type=MEDIATYPES[fmt])

    service.__doc__ = __doc__
