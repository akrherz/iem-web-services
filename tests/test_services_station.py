"""Test the station service."""

from fastapi.testclient import TestClient


def test_basic(client: TestClient):
    """Test basic calls."""
    # defined from testdata
    req = client.get("/station/96404.json")
    assert req.status_code == 200
