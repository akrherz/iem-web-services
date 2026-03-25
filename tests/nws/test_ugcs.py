"""Test the nws/ugc service."""

from fastapi.testclient import TestClient


def test_basic(client: TestClient):
    """Test basic calls."""
    req = client.get("/nws/ugcs.json?wfo=DMX&state=IA")
    res = req.json()
    assert res is not None


def test_time_call(client: TestClient):
    """Test basic calls."""
    req = client.get("/nws/ugcs.json?valid=2020-01-01T12:00")
    assert req.status_code == 200


def test_firewx(client: TestClient):
    """Test calling with is_firewx."""
    req = client.get("/nws/ugcs.json?just_firewx=1")
    assert req.status_code == 200
