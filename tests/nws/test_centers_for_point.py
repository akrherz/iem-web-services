"""Test the nws/centers_for_point service."""

from fastapi.testclient import TestClient


def test_basic(client: TestClient):
    """Test basic calls."""
    service = "/nws/centers_for_point.json?lat=43&lon=-95"
    req = client.get(service)
    assert req.status_code == 200
