"""Test the nws/taf_overview service."""

from fastapi.testclient import TestClient

from iemws.main import app

client = TestClient(app)


def test_overview_at():
    """Test a request at a given timestamp."""
    resp = client.get("/nws/taf_overview.json?at=2023-01-01T00:00:00Z")
    assert resp.status_code == 200


def test_too_large_request():
    """Test too big of a request."""
    resp = client.get(
        "/nws/taf_overview.json?sts=2022-01-01T00:00&ets=2022-11-01T00:00"
    )
    assert resp.status_code == 422


def test_too_large_request_station():
    """Test too big of a request."""
    resp = client.get(
        "/nws/taf_overview.json?sts=2022-01-01T00:00&ets=2024-01-01T00:00"
        "&station=KDSM"
    )
    assert resp.status_code == 422


def test_ok_large_station_request():
    """Test a large station request."""
    resp = client.get(
        "/nws/taf_overview.json?sts=2022-01-01T00:00&ets=2022-12-01T00:00"
        "&station=KDSM"
    )
    assert resp.status_code == 200


def test_basic():
    """Test basic calls."""
    resp = client.get("/nws/taf_overview.geojson")
    res = resp.json()
    assert res is not None
    assert resp.status_code == 200
