"""Test iowa_winter_roadcond."""

# third party
from fastapi.testclient import TestClient

# Local
from iemws.main import app

client = TestClient(app)


def test_invalid_date():
    """Test too soon date."""
    res = client.get(
        "/iowa_winter_roadcond.geojson", params={"valid": "1800-01-01T12:00Z"}
    )
    assert res.status_code == 422


def test_first():
    """Test we can do things."""
    res = client.get(
        "/iowa_winter_roadcond.geojson", params={"valid": "2021-01-01T12:00Z"}
    )
    j = res.json()
    assert "features" in j


def test_json():
    """Test we can do things."""
    res = client.get(
        "/iowa_winter_roadcond.json", params={"valid": "2021-01-01T12:00"}
    )
    j = res.json()
    assert "data" in j
