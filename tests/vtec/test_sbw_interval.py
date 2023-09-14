"""Test the vtec/sbw_interval service."""
# third party
from fastapi.testclient import TestClient

# Local
from iemws.main import app

client = TestClient(app)


def test_230914_failure():
    """Test a production failure..."""
    # https://mesonet.agron.iastate.edu/api/1/vtec/sbw_interval.geojson?begints=2023-08-30T00%3A00%3A00Z&endts=2023-08-31T00%3A00%3A00Z&only_new=true&ph=TO
    url = (
        "/vtec/sbw_interval.geojson?begints=2023-08-30T00%3A00%3A00Z&"
        "endts=2023-08-31T00%3A00%3A00Z&only_new=true&ph=TO"
    )
    req = client.get(url)
    assert req.status_code == 200
    res = req.json()
    assert res is not None


def test_basic():
    """Test basic calls."""
    url = "/vtec/sbw_interval?begints=2020-01-01T00:00&endts=2020-01-02T00:00"
    req = client.get(url)
    res = req.json()
    assert res is not None

    req = client.get(f"{url}&wfo=FWD&only_new=false&ph=SV")
    res = req.json()
    assert res is not None
