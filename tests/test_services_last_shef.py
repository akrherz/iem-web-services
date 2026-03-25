"""Test the last_shef service."""

from fastapi.testclient import TestClient


def test_valid_request(client: TestClient):
    """Test valid request."""
    req = client.get("/last_shef.txt?station=KDMX")
    assert req.text.startswith("station")
