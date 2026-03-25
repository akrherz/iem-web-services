"""Test SCP."""

from fastapi.testclient import TestClient


def test_first(client: TestClient):
    """Test we can do things."""
    res = client.get(
        "/scp.json", params={"station": "KAMW", "date": "2020-10-26"}
    )
    j = res.json()
    assert "data" in j
