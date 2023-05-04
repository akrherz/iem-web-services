"""Test the nws/taf_overview service."""

from fastapi.testclient import TestClient
from iemws.main import app

client = TestClient(app)


def test_basic():
    """Test basic calls."""
    req = client.get("/nws/taf_overview.geojson")
    res = req.json()
    assert res is not None
