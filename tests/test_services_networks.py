"""Test the networks service."""

from fastapi.testclient import TestClient
from iemws.main import app

client = TestClient(app)


def test_basic():
    """Test basic things."""
    req = client.get("/networks.json")
    res = req.json()
    assert res is not None
