"""Test the mos service."""

# third party
from fastapi.testclient import TestClient

# local
from iemws.main import app

client = TestClient(app)


def test_basic():
    """Test."""
    req = client.get("/mos.json?station=KDSM&model=GFS")
    assert req.status_code == 404


def test_bad_model():
    """Test with bad model."""
    req = client.get("/mos.json?station=KDSM&model=xxx")
    assert req.status_code == 422
