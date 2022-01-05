"""Test the vtec/county_zone service."""
# third party
from fastapi.testclient import TestClient

# Local
from iemws.main import app

client = TestClient(app)


def test_basic():
    """Test basic calls."""
    req = client.get("/vtec/county_zone.geojson")
    res = req.json()
    assert res is not None

    req = client.get("/vtec/county_zone.json?valid=2021-01-01T00:00")
    res = req.json()
    assert res is not None
