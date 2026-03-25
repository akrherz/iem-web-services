"""Test the networks service."""

from fastapi.testclient import TestClient


def test_basic(client: TestClient):
    """Test basic things."""
    req = client.get("/networks.json")
    res = req.json()
    assert res is not None
