"""Test the vtec/sbw_interval service."""

from fastapi.testclient import TestClient

from iemws.main import app

client = TestClient(app)


def test_basic():
    """Test basic calls."""
    url = "/vtec/events_status.json?valid=2023-02-03T02:30"
    req = client.get(url)
    assert req.json()["data"][0]["year"] == 2023  # Included with test data
    assert req.status_code == 200

    url = "/vtec/events_status.json?wfo=MEG"
    req = client.get(url)
    assert req.status_code == 200
