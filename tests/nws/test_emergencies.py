"""Test the nws/emergencies service."""

from fastapi.testclient import TestClient


def test_basic(client: TestClient):
    """Test basic calls."""
    service = "/nws/emergencies.geojson"
    resp = client.get(service)
    assert resp.status_code == 200
    payload = resp.json()
    url = payload["features"][0]["properties"]["uri"]
    assert url.find(".") == -1
    assert payload["features"][0]["properties"]["year"]
