"""Run some tests."""

from fastapi.testclient import TestClient

from iemws.main import app

client = TestClient(app, raise_server_exceptions=False)


def test_servertime():
    """Test servertime response."""
    response = client.get("/servertime")
    assert response.status_code == 200
    assert response.json().startswith("2")


def test_servertime_fail():
    """Test servertime response."""
    response = client.get("/servertime?opt=fail")
    assert response.status_code == 500
