"""Test the twis service."""

from fastapi.testclient import TestClient


def test_realtime(client: TestClient):
    """Exercise this special selector."""
    resp = client.get("/rwis.json")
    assert resp.status_code == 200


def test_archive(client: TestClient):
    """Test that we can query by station."""
    resp = client.get("/rwis.geojson?valid=2024-07-01T12:00:00Z")
    assert resp.status_code == 200


def test_archive_notz(client: TestClient):
    """Test request without timezone set."""
    resp = client.get("/rwis.geojson?valid=2024-07-01T12:00:00")
    assert resp.status_code == 200
