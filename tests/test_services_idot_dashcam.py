"""Test idot_dashcam."""

# third party
from fastapi.testclient import TestClient

# Local
from iemws.main import app

client = TestClient(app)


def test_first():
    """Test we can do things."""
    res = client.get(
        "/idot_dashcam.geojson", params={"valid": "2021-01-01T12:00Z"}
    )
    j = res.json()
    assert "features" in j


def test_json():
    """Test we can do things."""
    res = client.get(
        "/idot_dashcam.json", params={"valid": "2021-01-01T12:00"}
    )
    j = res.json()
    assert "data" in j
