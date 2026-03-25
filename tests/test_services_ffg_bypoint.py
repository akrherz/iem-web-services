"""Test the ffg_bypoint service."""

from fastapi.testclient import TestClient


def test_basic(client: TestClient):
    """Test that we need not provide a valid"""
    resp = client.get("/ffg_bypoint.json?lon=-81.69&lat=27.99")
    assert resp.status_code == 404


def test_nulls(client: TestClient):
    """Test for something that has masked/null values."""
    uri = "/ffg_bypoint.json?lon=-81.69&lat=27.99&valid=2023-07-13T00:00Z"
    res = client.get(uri).json()
    assert res["ffg"][4]["ffg_mm"] is None


def test_non_existant_gribfile(client: TestClient):
    """Test for a non-existant grib file."""
    uri = "/ffg_bypoint.json?lon=-81.69&lat=27.99&valid=2020-07-13T00:00Z"
    res = client.get(uri)
    assert res.status_code == 404
