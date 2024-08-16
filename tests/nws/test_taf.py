"""Test the nws/taf service."""

from fastapi.testclient import TestClient

from iemws.main import app

client = TestClient(app)


def test_basic():
    """Test basic calls."""
    req = client.get("/nws/taf.json?station=KDSM")
    res = req.json()
    assert res is not None


def test_text():
    """Test basic calls."""
    req = client.get("/nws/taf.txt?station=KDSM")
    assert req.status_code == 200
