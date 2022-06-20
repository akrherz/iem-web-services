"""Test the nws/spc_outlook service."""
# third party
from fastapi.testclient import TestClient

# Local
from iemws.main import app

client = TestClient(app)


def test_basic():
    """Test basic calls."""
    req = client.get(
        "/nws/spc_outlook.geojson?day=1&cycle=13&valid=2019-01-01",
    )
    res = req.json()
    assert "features" in res
