"""Test the obhistory service."""

from fastapi.testclient import TestClient

from iemws.main import app

client = TestClient(app)


def test_basic():
    """Test that we can walk."""
    req = client.get("/obhistory.json")
    res = req.json()
    assert res is not None


def test_dcp():
    """Test a DCP station request."""
    req = client.get(
        "/obhistory.json",
        params={"network": "IA_DCP", "station": "DNKI4", "date": "2020-08-08"},
    )
    res = req.json()
    assert not res["data"]


def test_uscrn_has_data():
    """Test USCRN request for historical data."""
    req = client.get(
        "/obhistory.json",
        params={"network": "USCRN", "station": "96404", "date": "2020-08-08"},
    )
    res = req.json()
    assert len(res["data"]) == 252


def test_uscrn_has_nodata():
    """Test USCRN request for historical data."""
    req = client.get(
        "/obhistory.json",
        params={"network": "USCRN", "station": "96404", "date": "2020-08-01"},
    )
    res = req.json()
    assert len(res["data"]) == 0
