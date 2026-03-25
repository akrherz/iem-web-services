"""Run some tests."""

from fastapi.testclient import TestClient


def test_servertime(client: TestClient):
    """Test servertime response."""
    response = client.get("/servertime")
    assert response.status_code == 200
    assert response.json().startswith("2")
