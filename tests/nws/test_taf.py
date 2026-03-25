"""Test the nws/taf service."""

from fastapi.testclient import TestClient


def test_basic(client: TestClient):
    """Test basic calls."""
    resp = client.get("/nws/taf.json?station=KDSM&issued=2026-02-13T12:00")
    res = resp.json()
    assert res is not None


def test_text(client: TestClient):
    """Test basic calls."""
    req = client.get("/nws/taf.txt?station=KDSM")
    assert req.status_code == 200
