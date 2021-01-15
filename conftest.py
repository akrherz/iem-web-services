"""Useful for testing."""
# local
import os

# third party
import pytest


@pytest.fixture()
def prodtest():
    """Are we testing against IEM Production database?"""
    return os.environ.get("HAS_IEMDATABASE") == "1"
