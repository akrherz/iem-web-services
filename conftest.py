"""Useful for testing."""

import os

import pytest
from fastapi.testclient import TestClient

from iemws.main import app
from iemws.services import drydown

# Make testing fasture
drydown.NCOPEN_TIMEOUT = 0.01


@pytest.fixture()
def client():
    # Context manager keeps one event loop for the test and runs the lifespan
    # so that async database pools are closed instead of leaked.
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture()
def prodtest():
    """Are we testing against IEM Production database?"""
    return os.environ.get("HAS_IEMDATABASE") == "1"
