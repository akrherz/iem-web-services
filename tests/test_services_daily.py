"""Test the daily service."""

from fastapi.testclient import TestClient

from iemws.main import app

client = TestClient(app)


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
