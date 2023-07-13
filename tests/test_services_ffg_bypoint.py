"""Test the ffg_bypoint service."""

from fastapi.testclient import TestClient
from iemws.main import app

client = TestClient(app)


def test_basic():
    """Test that we need not provide a WFO."""
    req = client.get("/ffg_bypoint.json")
    res = req.json()
    assert res is not None


def test_nulls():
    """Test for something that has masked/null values."""
    uri = "/ffg_bypoint.json?lon=-81.69&lat=27.99&valid=2023-07-13T00:00Z"
    res = client.get(uri).json()
    assert res["ffg"][4]["ffg_mm"] is None
