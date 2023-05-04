"""Test the shef_currents service."""

from fastapi.testclient import TestClient
from iemws.main import app

client = TestClient(app)


def test_basic():
    """Test that we can walk."""
    params = {
        "pe": "TA",
        "duration": "D",
    }
    req = client.get("/shef_currents.json", params=params)
    assert req.status_code == 200
