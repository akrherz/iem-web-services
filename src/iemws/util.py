"""Helpers."""

import inspect
import logging
import os
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager, contextmanager
from functools import wraps
from io import BytesIO
from typing import Callable

import psycopg
from fastapi import Response
from pandas import DataFrame
from pandas.api.types import is_datetime64_any_dtype as isdt
from pyiem.database import get_dbconnstr as pyiem_get_dbconnstr
from pyiem.reference import ISO8601
from sqlalchemy import engine

from .models import SupportedFormats
from .reference import MEDIATYPES

LOG = logging.getLogger("iemws")
DBFAIL_LOG_INTERVAL = 60  # seconds between logged database failures
_DBFAIL_LAST_LOGGED: float | None = None


def log_database_failure(exc: Exception) -> bool:
    """Log a database connectivity failure at most once per interval.

    A transient outage can fail thousands of requests, so only the first
    failure within the interval is logged, and without a traceback.

    Returns:
      bool: True if a line was logged.
    """
    global _DBFAIL_LAST_LOGGED  # noqa: PLW0603
    now = time.monotonic()
    if (
        _DBFAIL_LAST_LOGGED is not None
        and now - _DBFAIL_LAST_LOGGED < DBFAIL_LOG_INTERVAL
    ):
        return False
    _DBFAIL_LAST_LOGGED = now
    LOG.error(
        "Database unavailable (%s), suppressing similar messages for %ss",
        " ".join(str(exc).split()) or type(exc).__name__,
        DBFAIL_LOG_INTERVAL,
    )
    return True


def cache_control(max_age: int):
    """Add cache control headers to response."""

    def decorator(func: Callable):
        if inspect.iscoroutinefunction(func):

            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                res = await func(*args, **kwargs)
                if isinstance(res, Response):
                    res.headers["Cache-Control"] = f"public, max-age={max_age}"
                return res

            return async_wrapper

        @wraps(func)
        def wrapper(*args, **kwargs):
            res = func(*args, **kwargs)
            if isinstance(res, Response):
                res.headers["Cache-Control"] = f"public, max-age={max_age}"
            return res

        return wrapper

    return decorator


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
                (
                    df.set_crs("EPSG:4326", allow_override=True).to_file(
                        tmp, driver="GeoJSON", engine="pyogrio"
                    )
                )
                res = tmp.getvalue()
    return Response(res, media_type=MEDIATYPES[fmt])


def _get_pg_dbconnstr(name: str, rw: bool | None = False) -> str:
    """Get a plain libpq connection string, honoring env overrides."""
    host = os.getenv(f"IEMWS_DBHOST_{name.upper()}") or os.getenv(
        "IEMWS_DBHOST"
    )
    user = os.getenv("IEMWS_DBUSER")
    kwargs = {"rw": rw}
    if host:
        kwargs["host"] = host
    if user:
        kwargs["user"] = user
    return pyiem_get_dbconnstr(name, **kwargs)


def get_dbconnstr(name: str, rw: bool | None = False) -> str:
    """Get a SQLAlchemy database connection string using psycopg.

    Args:
      name (str): The name of the database to connect to
      rw (bool | None): Should a read-write connection be required?
    """
    return _get_pg_dbconnstr(name, rw=rw).replace(
        "postgresql:", "postgresql+psycopg:"
    )


@asynccontextmanager
async def get_async_conn(
    name: str, rw: bool | None = False
) -> AsyncGenerator[psycopg.AsyncConnection]:
    """Return a context managed async psycopg connection with UTC timezone.

    Args:
      name (str): The name of the database to connect to
      rw (bool | None): Should a read-write connection be required?
    """
    async with await psycopg.AsyncConnection.connect(
        _get_pg_dbconnstr(name, rw=rw),
        options="-c TimeZone=UTC",
        connect_timeout=5,
    ) as conn:
        yield conn


@contextmanager
def get_sqlalchemy_conn(name: str, rw: bool | None = False):
    """Return a context managed sqlalchemy connection."""
    # create a sqlalchemy connection with a default timezone of UTC set
    # https://stackoverflow.com/questions/26105730
    pgconn = engine.create_engine(
        get_dbconnstr(name, rw=rw),
        connect_args={"options": "-c TimeZone=UTC"},
    )
    yield pgconn
    pgconn.dispose()
