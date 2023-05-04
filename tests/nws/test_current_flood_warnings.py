"""Test the nws/current_Flood_warnings service."""

from fastapi.testclient import TestClient
from iemws.main import app

client = TestClient(app)


def test_basic():
    """Test basic calls."""
    req = client.get("/nws/current_flood_warnings.json")
    res = req.json()
    assert res is not None
