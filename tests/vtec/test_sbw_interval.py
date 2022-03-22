"""Test the vtec/sbw_interval service."""
# third party
from fastapi.testclient import TestClient

# Local
from iemws.main import app

client = TestClient(app)


def test_basic():
    """Test basic calls."""
    url = "/vtec/sbw_interval?begints=2020-01-01T00:00&endts=2020-01-02T00:00"
    req = client.get(url)
    res = req.json()
    assert res is not None

    req = client.get(f"{url}&wfo=FWD&only_new=false&ph=SV")
    res = req.json()
    assert res is not None
