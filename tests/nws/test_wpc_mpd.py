"""Test the nws/wpc_mpd service."""

from fastapi.testclient import TestClient


def test_basic(client: TestClient):
    """Test basic calls."""
    req = client.get("/nws/wpc_mpd.geojson?hours=6")
    res = req.json()
    assert res is not None

    req = client.get("/nws/wpc_mpd.geojson?valid=2021-01-01T00:00")
    res = req.json()
    assert res is not None
