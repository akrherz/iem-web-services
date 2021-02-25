"""IEM Station Metadata for One Network.

The IEM organizes stations into networks.  This service returns station
metadata for a given network.
"""
import tempfile

import pandas as pd
from geopandas import read_postgis
from fastapi import Query, Response
from ..models import SupportedFormats
from ..reference import MEDIATYPES
from ..util import get_dbconn


def handler(network_id, fmt):
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
    if fmt != "geojson":
        df = df.drop("geom", axis=1)
        df = pd.DataFrame(df)
    if fmt == "txt":
        return df.to_csv(index=False)
    if fmt == "json":
        return df.to_json(orient="table", index=False)
    with tempfile.NamedTemporaryFile("w", delete=True) as tmp:
        df.to_file(tmp.name, driver="GeoJSON")
        with open(tmp.name) as fh:
            res = fh.read()
    return res


def factory(app):
    """Generate."""

    @app.get("/network/{network_id}.{fmt}", description=__doc__)
    def usdm_bypoint_service(
        fmt: SupportedFormats,
        network_id: str = Query(..., description="IEM Network Identifier."),
    ):
        """Replaced above."""
        return Response(handler(network_id, fmt), media_type=MEDIATYPES[fmt])

    usdm_bypoint_service.__doc__ = __doc__
