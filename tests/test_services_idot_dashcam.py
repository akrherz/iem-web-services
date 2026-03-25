"""Test idot_dashcam."""

from fastapi.testclient import TestClient


def test_first(client: TestClient):
    """Test we can do things."""
    res = client.get(
        "/idot_dashcam.geojson", params={"valid": "2021-01-01T12:00Z"}
    )
    j = res.json()
    assert "features" in j


def test_json(client: TestClient):
    """Test we can do things."""
    res = client.get(
        "/idot_dashcam.json", params={"valid": "2021-01-01T12:00"}
    )
    j = res.json()
    assert "data" in j
