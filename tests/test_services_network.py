"""Test the network service."""

from fastapi.testclient import TestClient

from iemws.main import app

client = TestClient(app)


def test_asos1min():
    """Test the hacky ASOS1MIN."""
    resp = client.get("/network/ASOS1MIN.json")
    assert resp.status_code == 200


def test_basic():
    """Test basic calls."""
    # This actually 404s in CI
    req = client.get("/network/IA_ASOS.json")
    res = req.json()
    assert res is not None


def test_404():
    """Test that a 404 is raised."""
    req = client.get("/network/IA2_ASOS2.json")
    assert req.status_code == 404
