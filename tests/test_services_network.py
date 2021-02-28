"""Test the network service."""

from fastapi.testclient import TestClient

from iemws.main import app

client = TestClient(app)


def test_basic():
    """Test basic calls."""
    req = client.get("/network/IA_ASOS.json")
    res = req.json()
    assert res is not None


def test_404():
    """Test that a 404 is raised."""
    req = client.get("/network/IA2_ASOS2.json")
    assert not req
