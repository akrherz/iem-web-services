"""Test the nws/ugc service."""

from fastapi.testclient import TestClient
from iemws.main import app

client = TestClient(app)


def test_basic():
    """Test basic calls."""
    req = client.get("/nws/ugcs.json?wfo=DMX&state=IA")
    res = req.json()
    assert res is not None


def test_time_call():
    """Test basic calls."""
    req = client.get("/nws/ugcs.json?valid=2020-01-01T12:00")
    assert req.status_code == 200


def test_firewx():
    """Test calling with is_firewx."""
    req = client.get("/nws/ugcs.json?just_firewx=1")
    assert req.status_code == 200
