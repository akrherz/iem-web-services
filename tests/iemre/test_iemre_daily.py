"""Test iemre/daily service."""

from fastapi.testclient import TestClient


def test_daily_out_of_bounds(client: TestClient):
    """Test a call for outside of bounds point."""
    params = {
        "lon": 48.87,
        "lat": 2.3,
        "date": "2020-09-15",
    }
    res = client.get(
        "/iemre/daily.json",
        params=params,
    )
    assert res.status_code == 422
