"""Test isusm/daily"""

from fastapi.testclient import TestClient


def test_basic(client: TestClient):
    """Test basic things."""
    req = client.get("/isusm/daily.json?sdate=2003-01-01")
    res = req.json()
    assert res is not None


def test_isusm(client: TestClient):
    """Test ISUSM call."""
    req = client.get("/isusm/daily.json?sdate=2021-01-01")
    res = req.json()
    assert res is not None
