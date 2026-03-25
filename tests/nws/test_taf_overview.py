"""Test the nws/taf_overview service."""

from fastapi.testclient import TestClient


def test_overview_at(client: TestClient):
    """Test a request at a given timestamp."""
    resp = client.get("/nws/taf_overview.json?at=2023-01-01T00:00:00Z")
    assert "data" in resp.json()
    assert resp.status_code == 200


def test_too_large_request(client: TestClient):
    """Test too big of a request."""
    resp = client.get(
        "/nws/taf_overview.json?sts=2022-01-01T00:00&ets=2022-11-01T00:00"
    )
    assert resp.status_code == 422


def test_too_large_request_station(client: TestClient):
    """Test too big of a request."""
    resp = client.get(
        "/nws/taf_overview.json?sts=2022-01-01T00:00&ets=2024-01-01T00:00"
        "&station=KDSM"
    )
    assert resp.status_code == 422


def test_ok_large_station_request(client: TestClient):
    """Test a large station request."""
    resp = client.get(
        "/nws/taf_overview.json?sts=2022-01-01T00:00&ets=2022-12-01T00:00"
        "&station=KDSM"
    )
    assert "data" in resp.json()
    assert resp.status_code == 200


def test_basic(client: TestClient):
    """Test basic calls."""
    resp = client.get("/nws/taf_overview.geojson")
    assert "features" in resp.json()
    assert resp.status_code == 200
