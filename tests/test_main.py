"""Run some tests."""
from fastapi.testclient import TestClient

from iemws.main import app

client = TestClient(app)


def test_servertime():
    """Test servertime response."""
    response = client.get("/servertime")
    assert response.status_code == 200
    assert response.json().startswith("2")
