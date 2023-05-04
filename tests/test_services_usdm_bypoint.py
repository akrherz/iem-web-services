"""Test the daily service."""

from fastapi.testclient import TestClient
from iemws.main import app

client = TestClient(app)


def test_basic():
    """Test that we can walk."""
    params = {
        "sdate": "2019-01-01",
        "edate": "2019-02-01",
        "lon": -95.1,
        "lat": 42.3,
    }
    req = client.get("/usdm_bypoint.json", params=params)
    assert req.status_code == 200
