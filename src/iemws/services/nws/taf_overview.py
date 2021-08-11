"""Terminal Aerodome Forecast (TAF) Overview.

This service provides an overview of all presently available and most recent
TAF issuances as processed and parsed by the IEM.  The attributes for each
forecast include references to API endpoints providing either the raw
text (`text_href`) or JSON representation (`data_href`).  Additionally a
forecast aggregate of `min_visibility` (miles) is provided.
"""
import tempfile

import pandas as pd
from geopandas import read_postgis
from fastapi import Response, APIRouter
from ...models import SupportedFormats
from ...reference import MEDIATYPES
from ...util import get_dbconn

router = APIRouter()


def handler(fmt):
    """Handle the request."""
    pgconn = get_dbconn("asos")
    df = read_postgis(
        """
        with forecasts as (
            select station, id, valid as issuance, product_id,
            row_number() OVER (PARTITION by station ORDER by valid DESC)
            from taf where valid > now() - '24 hours'::interval and
            valid < now()),
        agg as (
            select station, id, issuance, product_id from forecasts
            where row_number = 1),
        stinfo as (
            select a.*, t.geom, t.name from agg a JOIN stations t on (
                (case when substr(a.station, 1, 1) = 'K'
                 then substr(a.station, 2, 3) else a.station end) = t.id)
            WHERE t.network = 'AWOS' or t.network ~* 'ASOS'),
        agg2 as (
            select a.id, min(visibility) as min_visibility
            from agg a JOIN taf2021 t on (a.id = t.taf_id) GROUP by a.id)

        select s.station, s.product_id, s.geom, s.name,
        to_char(s.issuance at time zone 'UTC', 'YYYY-MM-DDThh24:MI:SSZ')
            as utc_issued,
        ST_X(geom) as lon, ST_Y(geom) as lat,
        a.min_visibility from stinfo s JOIN agg2 a on (s.id = a.id)
        """,
        pgconn,
        geom_col="geom",
        index_col=None,
    )
    df["data_href"] = (
        "/api/1/nws/taf.json?station="
        + df["station"]
        + "&issued="
        + df["utc_issued"]
    )
    df["text_href"] = "/api/1/nwstext/" + df["product_id"].str.strip()

    if fmt != "geojson":
        df = pd.DataFrame(df.drop("geom", axis=1))
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


@router.get("/nws/taf_overview.{fmt}", description=__doc__)
def service(
    fmt: SupportedFormats,
):
    """Replaced above."""
    return Response(handler(fmt), media_type=MEDIATYPES[fmt])


service.__doc__ = __doc__
