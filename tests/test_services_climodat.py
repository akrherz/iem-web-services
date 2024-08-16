"""Test the climodat service."""

from fastapi.testclient import TestClient

from iemws.main import app

client = TestClient(app)


def test_basic():
    """Test that we need not provide a WFO."""
    req = client.get("/climodat/por_daily_climo.json?station=IA9999")
    res = req.json()
    assert res is not None


def test_ames():
    """Test that we can get data for a known station."""
    req = client.get("/climodat/por_daily_climo.json?station=IATAME")
    res = req.json()
    assert len(res["data"]) == 366
