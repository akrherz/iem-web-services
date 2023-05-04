"""Test the last_shef service."""

from fastapi.testclient import TestClient
from iemws.main import app

client = TestClient(app)


def test_valid_request():
    """Test valid request."""
    req = client.get("/last_shef.txt?station=KDMX")
    assert req.text.startswith("station")
