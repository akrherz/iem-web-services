"""Helpers."""
import tempfile

from pandas import DataFrame
from pyiem import util
from fastapi import HTTPException, Response
from .models import SupportedFormats
from .reference import MEDIATYPES


def deliver_df(df: DataFrame, fmt: str):
    """Standard DataFrame delivery for fastapi."""
    res = ""
    if fmt != SupportedFormats.geojson:
        if "geom" in df.columns:
            # Means to covert a GeoDataFrame to DataFrame
            df = DataFrame(df.drop("geom", axis=1))
    if fmt == SupportedFormats.json:
        res = df.to_json(orient="table", default_handler=str)
    elif fmt == SupportedFormats.txt:
        res = df.to_csv(index=False)
    elif fmt == SupportedFormats.geojson:
        if df.empty:
            res = '{"type": "FeatureCollection", "features": []}'
        else:
            with tempfile.NamedTemporaryFile("w", delete=True) as tmp:
                df.to_file(tmp.name, driver="GeoJSON")
                with open(tmp.name, encoding="utf8") as fh:
                    res = fh.read()
    return Response(res, media_type=MEDIATYPES[fmt])


def get_dbconn(name):
    """Get a database connection or fail."""
    try:
        return util.get_dbconn(name)
    except Exception:
        # pylint: disable=raise-missing-from
        raise HTTPException(status_code=503, detail="Database unavailable.")
