"""Test the nws/spc_outlook service."""

from fastapi.testclient import TestClient


def test_basic(client: TestClient):
    """Test basic calls."""
    req = client.get(
        "/nws/spc_outlook.geojson?day=1&cycle=13&valid=2019-01-01",
    )
    res = req.json()
    assert "features" in res
