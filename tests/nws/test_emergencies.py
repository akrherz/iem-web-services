"""Test the nws/emergencies service."""

from fastapi.testclient import TestClient
from iemws.main import app

client = TestClient(app)


def test_basic():
    """Test basic calls."""
    service = "/nws/emergencies.json"
    req = client.get(service)
    assert req.status_code == 200
