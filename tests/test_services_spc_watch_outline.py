"""Test spc_watch_outline."""

from fastapi.testclient import TestClient


def test_first(client: TestClient):
    """Test we can do things."""
    res = client.get(
        "/spc_watch_outline.geojson", params={"valid": "2024-05-21T12:00:00Z"}
    )
    j = res.json()
    assert "features" in j
