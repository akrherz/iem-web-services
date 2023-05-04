"""Test the station service."""

from fastapi.testclient import TestClient
from iemws.main import app

client = TestClient(app)


def test_basic():
    """Test basic calls."""
    # defined from testdata
    req = client.get("/station/96404.json")
    assert req.status_code == 200
