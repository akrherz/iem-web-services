"""Unused at the moment."""
import os

from fastapi.testclient import TestClient

from ..main import app

client = TestClient(app)


def test_currents():
    """Test we can get currents."""
    res = client.get("/currents.json", params={"network": "IA_ASOS"})
    answer = 503 if os.environ.get("NODATABASE", "0") == "1" else 200
    assert res.status_code == answer


def test_empty():
    """Can we run when no data is found?"""
    response = client.get(
        "/cow.json",
        params={
            "wfo": "DMX",
            "begints": "2000-01-01T12:00Z",
            "endts": "2000-01-02T12:00Z",
        },
    )
    answer = 503 if os.environ.get("NODATABASE", "0") == "1" else 200
    assert response.status_code == answer
