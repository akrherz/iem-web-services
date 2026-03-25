"""Test the vtec/county_zone service."""

from fastapi.testclient import TestClient


def test_basic(client: TestClient):
    """Test basic calls."""
    req = client.get("/vtec/county_zone.geojson")
    res = req.json()
    assert res is not None

    req = client.get("/vtec/county_zone.json?valid=2021-01-01T00:00")
    res = req.json()
    assert res is not None
