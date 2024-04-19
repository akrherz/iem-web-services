"""Test the daily service."""

from fastapi.testclient import TestClient
from iemws.main import app

client = TestClient(app)


def test_pre1928_request():
    """Test that this is handled."""
    req = client.get(
        "/daily.json?station=MMLW1&network=WA_DCP&year=1920&month=3",
    )
    assert req.status_code == 500


def test_error500():
    """Test that an invalid request is caught."""
    req = client.get("/daily.json?network=IA_ASOS")
    assert req.status_code == 500


def test_basic():
    """Test that we need not provide a WFO."""
    req = client.get("/daily.json?year=2021&network=IA_ASOS&station=AMW")
    res = req.json()
    assert res is not None


def test_climate():
    """Test a climate station query."""
    req = client.get("/daily.json?year=2021&network=IACLIMATE&station=IATAME")
    res = req.json()
    assert res is not None


def test_climate_year_month():
    """Test a climate station query."""
    req = client.get(
        "/daily.json?year=2021&month=3&network=IACLIMATE&station=IATAME"
    )
    res = req.json()
    assert res is not None


def test_climate_date():
    """Test a climate station query."""
    req = client.get(
        "/daily.json?date=2023-01-01&network=IACLIMATE&station=IATAME"
    )
    res = req.json()
    assert res is not None


def test_asos_date():
    """Test a ASOS station query."""
    req = client.get("/daily.json?date=2023-01-01&network=IA_ASOS&station=AMW")
    assert req.status_code == 200


def test_asos_year_month():
    """Test a ASOS station query."""
    req = client.get(
        "/daily.json?year=2023&month=1&network=IA_ASOS&station=AMW"
    )
    assert req.status_code == 200
