"""Useful for testing."""

# local
import os

# third party
import pytest

# Local
from iemws.services import drydown

# Make testing fasture
drydown.NCOPEN_TIMEOUT = 0.01


@pytest.fixture()
def prodtest():
    """Are we testing against IEM Production database?"""
    return os.environ.get("HAS_IEMDATABASE") == "1"
