"""Test idot_rwiscam."""

from fastapi.testclient import TestClient


def test_first(client: TestClient):
    """Test we can do things."""
    res = client.get(
        "/idot_rwiscam.geojson", params={"valid": "2021-01-01T12:00Z"}
    )
    j = res.json()
    assert j["features"]


def test_empty(client: TestClient):
    """Test a request without data."""
    res = client.get(
        "/idot_rwiscam.geojson",
        params={"valid": "2003-01-01T12:00", "window": 0},
    )
    j = res.json()
    assert not j["features"]


def test_realtime(client: TestClient):
    """Test that a realtime request works."""
    res = client.get("/idot_rwiscam.geojson")
    j = res.json()
    assert "features" in j
