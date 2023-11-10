"""Test the nws/snowfall_6hour service."""

from fastapi.testclient import TestClient
from iemws.main import app

client = TestClient(app)


def test_basic():
    """Test basic calls."""
    service = "/nws/snowfall_6hour.json?valid=2023-11-10T12:00:00Z"
    req = client.get(service)
    assert req.status_code == 200
