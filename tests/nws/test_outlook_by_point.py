"""Test the nws/outlook_by_point service."""

from fastapi.testclient import TestClient


def test_basic(client: TestClient):
    """Test basic calls."""
    service = "/nws/outlook_by_point.json?lat=43&lon=-95"
    req = client.get(service)
    assert req.status_code == 200
    req = client.get(f"{service}&valid=2011-04-27T18:00")
    assert req.status_code == 200
