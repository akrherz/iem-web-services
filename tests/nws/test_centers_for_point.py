"""Test the nws/centers_for_point service."""

from fastapi.testclient import TestClient

from iemws.main import app

client = TestClient(app)


def test_basic():
    """Test basic calls."""
    service = "/nws/centers_for_point.json?lat=43&lon=-95"
    req = client.get(service)
    assert req.status_code == 200
