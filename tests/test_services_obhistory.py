"""Test the obhistory service."""

from datetime import date

from fastapi.testclient import TestClient

from iemws.main import app

client = TestClient(app)


def test_nstl():
    """Test that NSTL works."""
    resp = client.get(
        "/obhistory.json?station=NSTL11&network=NSTLFLUX&date=2000-01-01"
    )
    assert resp.status_code == 404
    assert resp.json()["detail"] == "No data found for request"


def test_cocorahs():
    """Test that cocorahs returns something."""
    req = client.get(
        "/obhistory.json?station=IA-PK-97&network=IA_COCORAHS&date=2007-01-01"
    )
    assert req.status_code == 200


def test_404():
    """Test that this generates a 404"""
    req = client.get("/obhistory.json?station=AMW&network=AI&date=2000-01-01")
    assert req.status_code == 404


def test_asos_archive():
    """Test that this works"""
    req = client.get(
        "/obhistory.json?station=AMW&network=IA_ASOS&date=2000-01-01"
    )
    assert req.status_code == 200


def test_isusm_archive():
    """Test that this works"""
    resp = client.get(
        "/obhistory.json?station=BOOI4&network=ISUSM&date=2024-07-21"
    )
    assert resp.json()["data"]


def test_rwis_archive():
    """Test that works"""
    req = client.get(
        "/obhistory.json?station=RAMI4&network=IA_RWIS&date=2000-01-01"
    )
    assert req.status_code == 200


def test_today():
    """Test a request for today's data."""
    req = client.get(
        f"/obhistory.json?date={date.today():%Y-%m-%d}&station=AMW&network=AI"
    )
    assert req.status_code == 200


def test_basic():
    """Test that we can walk."""
    req = client.get("/obhistory.json")
    res = req.json()
    assert res is not None


def test_dcp_alldata():
    """Test a query that should hit alldata."""
    req = client.get(
        "/obhistory.json",
        params={"network": "IA_DCP", "station": "DNKI4", "date": "2023-11-10"},
    )
    res = req.json()
    assert res["data"]


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
        params={
            "network": "USCRN",
            "station": "96404",
            "date": "2020-08-08",
            "full": True,
        },
    )
    res = req.json()
    assert len(res["data"]) == 252
    assert "vsby" in res["data"][0]


def test_uscrn_has_nodata():
    """Test USCRN request for historical data."""
    req = client.get(
        "/obhistory.json",
        params={"network": "USCRN", "station": "96404", "date": "2020-08-01"},
    )
    res = req.json()
    assert len(res["data"]) == 0


def test_snet_has_nodata():
    """Test SNET request for historical data."""
    req = client.get(
        "/obhistory.json",
        params={"network": "KELO", "station": "SRMS2", "date": "2020-08-01"},
    )
    res = req.json()
    assert len(res["data"]) == 0


def test_scan():
    """Test SCAN request."""
    req = client.get(
        "/obhistory.json",
        params={"network": "SCAN", "station": "S2031", "date": "2020-08-08"},
    )
    assert req.status_code == 200
