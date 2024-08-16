"""Test the trending_autoplots service."""

from fastapi.testclient import TestClient

from iemws.main import app

client = TestClient(app)


def test_basic():
    """Test simple."""
    req = client.get("/iem/trending_autoplots.json")
    assert req.status_code == 200
