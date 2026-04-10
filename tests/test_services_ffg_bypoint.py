"""Test the ffg_bypoint service."""

from datetime import timedelta

from fastapi.testclient import TestClient
from pyiem.util import utc


def test_basic(client: TestClient):
    """Test that we need not provide a valid"""
    resp = client.get("/ffg_bypoint.json?lon=-81.69&lat=27.99")
    # Quasi non-deterministic
    assert resp.status_code in [404, 200]


def test_nulls(client: TestClient):
    """Test for something that has masked/null values."""
    uri = "/ffg_bypoint.json?lon=-81.69&lat=27.99&valid=2023-07-13T00:00Z"
    res = client.get(uri).json()
    assert res["ffg"][4]["ffg_mm"] is None


def test_non_existant_gribfile(client: TestClient):
    """Test for a non-existant grib file."""
    future = utc() + timedelta(days=3)
    uri = (
        "/ffg_bypoint.json?lon=-81.69&lat=27.99"
        f"&valid={future:%Y-%m-%dT%H:%MZ}"
    )
    res = client.get(uri)
    assert res.status_code == 404
