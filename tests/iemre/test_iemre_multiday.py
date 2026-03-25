"""Test iemre/multiday service."""

from fastapi.testclient import TestClient


def test_multidaily_out_of_bounds(client: TestClient):
    """Test a call for outside of bounds point."""
    params = {
        "lon": 48.87,
        "lat": 2.3,
        "sdate": "2020-09-15",
        "edate": "2020-09-16",
    }
    res = client.get(
        "/iemre/multiday.json",
        params=params,
    )
    assert res.status_code == 422
