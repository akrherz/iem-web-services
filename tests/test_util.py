"""Tests for shared utilities."""

import logging

import psycopg
import pytest
from sqlalchemy.exc import OperationalError

from iemws import util


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
