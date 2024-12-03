"""Test iemre/multiday service."""

# third party
from fastapi.testclient import TestClient

# local
from iemws.main import app

client = TestClient(app)


def test_multidaily_out_of_bounds():
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
