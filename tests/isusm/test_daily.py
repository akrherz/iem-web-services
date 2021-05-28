"""Test isusm/daily"""

# third party
from fastapi.testclient import TestClient

# local
from iemws.main import app

client = TestClient(app)


def test_basic():
    """Test basic things."""
    req = client.get("/isusm/daily.json?sdate=2003-01-01")
    res = req.json()
    assert res is not None


def test_isusm():
    """Test ISUSM call."""
    req = client.get("/isusm/daily.json?sdate=2021-01-01")
    res = req.json()
    assert res is not None
