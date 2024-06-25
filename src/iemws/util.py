"""Helpers."""

import logging
from contextlib import contextmanager
from functools import wraps
from io import BytesIO
from typing import Callable

from fastapi import Request, Response
from fastapi.responses import JSONResponse
from pandas import DataFrame
from pandas.api.types import is_datetime64_any_dtype as isdt
from pyiem import util
from pyiem.reference import ISO8601
from sqlalchemy import engine

from .models import SupportedFormats
from .reference import MEDIATYPES

LOG = logging.getLogger("iemws")


def cache_control(max_age: int):
    """Add cache control headers to response."""

    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            res = func(*args, **kwargs)
            if isinstance(res, Response):
                res.headers["Cache-Control"] = f"public, max-age={max_age}"
            return res

        return wrapper

    return decorator


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
            df[column] = df[column].dt.strftime(ISO8601)
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
            with BytesIO() as tmp:
                df.crs = "EPSG:4326"
                df.to_file(tmp, driver="GeoJSON", engine="pyogrio")
                res = tmp.getvalue()
    return Response(res, media_type=MEDIATYPES[fmt])


@contextmanager
def get_sqlalchemy_conn(name):
    """Return a context managed sqlalchemy connection."""
    # create a sqlalchemy connection with a default timezone of UTC set
    # https://stackoverflow.com/questions/26105730
    pgconn = engine.create_engine(
        get_dbconnstr(name),
        connect_args={"options": "-c TimeZone=UTC"},
    )
    yield pgconn
    pgconn.dispose()


def get_dbconnstr(name):
    """Get a database connection string."""
    # 1. Allows us to specify the usage of psycopg as the module
    # 2. Sets the timezone to UTC
    return util.get_dbconnstr(name).replace(
        "postgresql:", "postgresql+psycopg:"
    )
