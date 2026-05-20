"""Test spc_watch_outline."""

from fastapi.testclient import TestClient


def test_first(client: TestClient):
    """Test we can do things."""
    resp = client.get(
        "/spc_watch_outline.geojson", params={"valid": "2024-05-21T12:00:00Z"}
    )
    assert resp.status_code == 200
    assert "features" in resp.json()
