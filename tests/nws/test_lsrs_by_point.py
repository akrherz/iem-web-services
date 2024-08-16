"""Test the nws/lsrs_by_point service."""

from fastapi.testclient import TestClient

from iemws.main import app

client = TestClient(app)


def test_search_by_meters_of_point():
    """Test search by meters of point."""
    service = (
        "/nws/lsrs_by_point.geojson?lat=43&lon=-94.23&meters=1000&"
        "begints=2018-06-20T21:56&endts=2018-06-20T21:57"
    )
    req = client.get(service)
    assert req.status_code == 200


def test_basic():
    """Test basic calls."""
    service = "/nws/lsrs_by_point.json?lat=43&lon=-95"
    req = client.get(service)
    assert req.status_code == 200


def test_testdata_bypoint():
    """Test things that should work for data found in testdata."""
    service = (
        "/nws/lsrs_by_point.geojson?lat=43&lon=-94.23&radius_miles=100&"
        "begints=2018-06-20T21:56&endts=2018-06-20T21:57"
    )
    res = client.get(service).json()
    assert len(res["features"]) == 1
