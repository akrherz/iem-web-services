"""Helpers."""
import logging
import os
import tempfile
from contextlib import contextmanager

from fastapi import Request, Response
from fastapi.responses import JSONResponse
from pandas import DataFrame
from pandas.api.types import is_datetime64_any_dtype as isdt
from pyiem import util
from sqlalchemy import engine

from .models import SupportedFormats
from .reference import MEDIATYPES

LOG = logging.getLogger("iemws")


def handle_exception(request: Request, exc):
    """Handle exceptions."""
    LOG.exception("Exception for %s", request.url, exc_info=exc)
    return JSONResponse(
        status_code=500,
        content="Unexpected error, email akrherz@iastate.edu if you wish :)",
    )


def deliver_df(df: DataFrame, fmt: str):
    """Standard DataFrame delivery for fastapi."""
    # Dragons: do timestamp conversion as pandas has many bugs
    for column in df.columns:
        if isdt(df[column]):
            df[column] = df[column].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    res = ""
    if fmt != SupportedFormats.geojson:
        if "geom" in df.columns:
            # Means to covert a GeoDataFrame to DataFrame
            df = DataFrame(df.drop("geom", axis=1))
    if fmt == SupportedFormats.json:
        res = df.to_json(
            orient="table",
            default_handler=str,
        )
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


@contextmanager
def get_sqlalchemy_conn(name):
    """Return a context managed sqlalchemy connection."""
    pgconn = engine.create_engine(get_dbconn(name))
    yield pgconn
    pgconn.dispose()


def get_dbconn(name):
    """Get a database connection string."""
    # Dragons: We set this now so that any subquent database reads will
    # properly load timestamptz column types into datetime objects
    os.environ["PGTZ"] = "UTC"
    # Le Sigh
    return util.get_dbconnstr(name).replace("postgresql", "postgresql+psycopg")
