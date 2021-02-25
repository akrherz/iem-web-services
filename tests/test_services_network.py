"""Test the network service."""

from fastapi.testclient import TestClient

from iemws.main import app

client = TestClient(app)


def test_basic():
    """Test basic calls."""
    req = client.get("/network/IA_ASOS.json")
    res = req.json()
    assert res is not None
