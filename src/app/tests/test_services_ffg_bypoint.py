"""Test the ffg_bypoint service."""

from fastapi.testclient import TestClient

from ..main import app

client = TestClient(app)


def test_basic():
    """Test that we need not provide a WFO."""
    req = client.get("/ffg_bypoint.json")
    res = req.json()
    assert res is not None
