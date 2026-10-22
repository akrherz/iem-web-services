"""Tests for shared utilities."""

import asyncio
import logging

import psycopg
import pytest
from sqlalchemy.exc import OperationalError

from iemws import util


def test_async_pools_are_scoped_to_event_loop(monkeypatch):
    """Keep pools registered until their owning event loop closes them."""

    class FakePool:
        def __init__(self):
            self.closed = False

        async def open(self):
            pass

        async def close(self):
            self.closed = True

    monkeypatch.setattr(
        util, "AsyncConnectionPool", lambda *args, **kwargs: FakePool()
    )
    first_loop = asyncio.new_event_loop()
    second_loop = asyncio.new_event_loop()
    try:
        first_pool = first_loop.run_until_complete(
            util._get_async_pool("iem", False)
        )
        second_pool = second_loop.run_until_complete(
            util._get_async_pool("iem", False)
        )

        assert first_pool is not second_pool

        second_loop.run_until_complete(util.close_async_pools())

        assert second_pool.closed
        assert not first_pool.closed
        assert util._ASYNC_POOLS[("iem", False, first_loop)] is first_pool

        first_loop.run_until_complete(util.close_async_pools())
        assert first_pool.closed
    finally:
        first_loop.close()
        second_loop.close()


def test_log_database_failure_includes_database_name(caplog, monkeypatch):
    """Include the database name from the failed connection in the log."""
    monkeypatch.setattr(util, "_DBFAIL_LAST_LOGGED", None)

    class FakeEngine:
        def dispose(self):
            pass

    monkeypatch.setattr(
        util.engine, "create_engine", lambda *args, **kwargs: FakeEngine()
    )
    with pytest.raises(OperationalError) as excinfo:
        with util.get_sqlalchemy_conn("iem"):
            raise OperationalError(
                "query",
                {},
                psycopg.OperationalError("connection timeout expired"),
            )

    with caplog.at_level(logging.ERROR, logger="iemws"):
        assert util.log_database_failure(excinfo.value)

    assert "Database iem unavailable" in caplog.text
    assert "connection timeout expired" in caplog.text
