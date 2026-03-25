"""Test the trending_autoplots service."""

from fastapi.testclient import TestClient


def test_basic(client: TestClient):
    """Test simple."""
    req = client.get("/iem/trending_autoplots.json")
    assert req.status_code == 200
