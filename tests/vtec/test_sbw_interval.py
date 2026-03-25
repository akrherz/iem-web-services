"""Test the vtec/sbw_interval service."""

from fastapi.testclient import TestClient


def test_no_params(client: TestClient):
    """Test with no parameters."""
    req = client.get("/vtec/sbw_interval.geojson")
    assert req.status_code == 200


def test_230914_failure(client: TestClient):
    """Test a production failure..."""
    # https://mesonet.agron.iastate.edu/api/1/vtec/sbw_interval.geojson?begints=2023-08-30T00%3A00%3A00Z&endts=2023-08-31T00%3A00%3A00Z&only_new=true&ph=TO
    url = (
        "/vtec/sbw_interval.geojson?begints=2023-08-30T00%3A00%3A00Z&"
        "endts=2023-08-31T00%3A00%3A00Z&only_new=true&ph=TO&include_can=0"
    )
    req = client.get(url)
    assert req.status_code == 200
    res = req.json()
    assert res is not None


def test_basic(client: TestClient):
    """Test basic calls."""
    url = "/vtec/sbw_interval?begints=2020-01-01T00:00&endts=2020-01-02T00:00"
    req = client.get(url)
    res = req.json()
    assert res is not None

    req = client.get(f"{url}&wfo=FWD&only_new=false&ph=SV")
    res = req.json()
    assert res is not None
