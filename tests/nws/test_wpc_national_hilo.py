"""Test the nws/wpc_national_hilo service."""

# third party
from fastapi.testclient import TestClient

# Local
from iemws.main import app

client = TestClient(app)


def test_basic():
    """Test basic calls."""
    req = client.get("/nws/wpc_national_hilo.json")
    assert req.status_code == 200

    req = client.get("/nws/wpc_national_hilo.txt?year=2022")
    assert req.status_code == 200

    req = client.get("/nws/wpc_national_hilo.json?state=IA")
    assert req.status_code == 200

    req = client.get("/nws/wpc_national_hilo.txt?state=IA&year=2022")
    assert req.status_code == 200
