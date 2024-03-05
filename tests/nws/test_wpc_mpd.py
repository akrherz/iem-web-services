"""Test the nws/wpc_mpd service."""

# third party
from fastapi.testclient import TestClient

# Local
from iemws.main import app

client = TestClient(app)


def test_basic():
    """Test basic calls."""
    req = client.get("/nws/wpc_mpd.geojson?hours=6")
    res = req.json()
    assert res is not None

    req = client.get("/nws/wpc_mpd.geojson?valid=2021-01-01T00:00")
    res = req.json()
    assert res is not None
