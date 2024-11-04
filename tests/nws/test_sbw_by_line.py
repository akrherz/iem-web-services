"""Test the nws/sbw_by_line service."""

from fastapi.testclient import TestClient

from iemws.main import app

client = TestClient(app)


def test_sbw_by_line():
    """Test."""
    service = (
        "/nws/sbw_by_line.json?start_lat=41.03&start_lon=-96.31&"
        "end_lat=41.04&end_lon=-96.21&begints=2024-07-02T07:15&"
        "endts=2024-07-02T09:57"
    )
    jdata = client.get(service).json()
    assert jdata["data"]


def test_sbw_by_line_include():
    """Test."""
    service = (
        "/nws/sbw_by_line.json?start_lat=41.03&start_lon=-96.31&"
        "end_lat=41.04&end_lon=-96.21&begints=2024-07-02T07:15&"
        "endts=2024-07-02T09:57&include_svs=1"
    )
    jdata = client.get(service).json()
    assert jdata["data"]
