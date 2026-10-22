"""Helpers."""

import asyncio
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
from psycopg_pool import AsyncConnectionPool
from pyiem.database import get_dbconnstr as pyiem_get_dbconnstr
from pyiem.reference import ISO8601
from sqlalchemy import engine
from sqlalchemy.exc import OperationalError as SAOperationalError

from .models import SupportedFormats
from .reference import MEDIATYPES

LOG = logging.getLogger("iemws")
DBFAIL_LOG_INTERVAL = 60  # seconds between logged database failures
_DBFAIL_LAST_LOGGED: float | None = None
ASYNC_POOL_MIN_SIZE = int(os.getenv("IEMWS_POOL_MIN", "2"))
ASYNC_POOL_MAX_SIZE = int(os.getenv("IEMWS_POOL_MAX", "20"))


def log_database_failure(
    exc: Exception, database_name: str | None = None
) -> bool:
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
    message = " ".join(str(exc).split()) or type(exc).__name__
    database_name = database_name or getattr(exc, "database_name", None)
    if database_name:
        LOG.error(
            "Database %s unavailable (%s), suppressing similar messages "
            "for %ss",
            database_name,
            message,
            DBFAIL_LOG_INTERVAL,
        )
    else:
        LOG.error(
            "Database unavailable (%s), suppressing similar messages for %ss",
            message,
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
                    df.set_crs("EPSG:4326", allow_override=True).to_file(  # type: ignore
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


_ASYNC_POOLS: dict[tuple[str, bool], tuple[AsyncConnectionPool, object]] = {}


async def _get_async_pool(name: str, rw: bool | None) -> AsyncConnectionPool:
    """Get (lazily creating) the async pool for this database."""
    key = (name, bool(rw))
    loop = asyncio.get_running_loop()
    entry = _ASYNC_POOLS.get(key)
    # A pool is bound to the event loop that opened it.  This only differs
    # in testing, where each request may get its own loop.
    if entry is not None and entry[1] is loop:
        return entry[0]
    pool = AsyncConnectionPool(
        _get_pg_dbconnstr(name, rw=rw),
        kwargs={"options": "-c TimeZone=UTC", "connect_timeout": 5},
        min_size=ASYNC_POOL_MIN_SIZE,
        max_size=ASYNC_POOL_MAX_SIZE,
        timeout=5,
        max_idle=300,
        open=False,
    )
    await pool.open()
    _ASYNC_POOLS[key] = (pool, loop)
    return pool


async def close_async_pools():
    """Close all pools, called at application shutdown."""
    entries = list(_ASYNC_POOLS.values())
    _ASYNC_POOLS.clear()
    for pool, loop in entries:
        if loop is asyncio.get_running_loop():
            await pool.close()


@asynccontextmanager
async def get_async_conn(
    name: str, rw: bool | None = False
) -> AsyncGenerator[psycopg.AsyncConnection]:
    """Return a context managed, pooled async psycopg connection (UTC).

    Args:
      name (str): The name of the database to connect to
      rw (bool | None): Should a read-write connection be required?
    """
    try:
        pool = await _get_async_pool(name, rw)
        async with pool.connection() as conn:
            yield conn
    except psycopg.OperationalError as exc:
        exc.database_name = name
        raise


@contextmanager
def get_sqlalchemy_conn(name: str, rw: bool | None = False):
    """Return a context managed sqlalchemy connection."""
    # create a sqlalchemy connection with a default timezone of UTC set
    # https://stackoverflow.com/questions/26105730
    pgconn = engine.create_engine(
        get_dbconnstr(name, rw=rw),
        connect_args={"options": "-c TimeZone=UTC"},
    )
    try:
        yield pgconn
    except (psycopg.OperationalError, SAOperationalError) as exc:
        exc.database_name = name
        raise
    finally:
        pgconn.dispose()
