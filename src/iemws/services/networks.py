"""IEM Networks Overview.

For better or worse, the IEM organizes station data into groups called
"networks".  These networks are often delineate political bounds and station
types.  One noticable one-off is the Iowa ASOS/AWOS data.  There is a
dedicated network called ``AWOS`` which represents the airport weather stations
within the state that are not maintained by the NWS+FAA.
"""
import tempfile

import pandas as pd
from geopandas import read_postgis
from fastapi import Response, APIRouter
from ..models import SupportedFormats
from ..reference import MEDIATYPES
from ..util import get_dbconn

router = APIRouter()


def handler(fmt):
    """Handle the request, return dict"""
    pgconn = get_dbconn("mesosite")

    df = read_postgis(
        "SELECT * from networks ORDER by id ASC",
        pgconn,
        geom_col="extent",
        index_col=None,
    )
    if fmt != "geojson":
        df = df.drop("extent", axis=1)
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


@router.get("/networks.{fmt}", description=__doc__)
def usdm_bypoint_service(
    fmt: SupportedFormats,
):
    """Replaced above."""
    return Response(handler(fmt), media_type=MEDIATYPES[fmt])


usdm_bypoint_service.__doc__ = __doc__
