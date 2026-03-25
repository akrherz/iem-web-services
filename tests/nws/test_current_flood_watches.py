"""Test the nws/current_Flood_warnings service."""

from fastapi.testclient import TestClient


def test_basic(client: TestClient):
    """Test basic calls."""
    req = client.get("/nws/current_flood_watches.json")
    res = req.json()
    assert res is not None
