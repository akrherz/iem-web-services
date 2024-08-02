"""Test the mos service."""

# third party
from fastapi.testclient import TestClient
from iemws.main import app

client = TestClient(app)


def test_faked_nbs():
    """Test data provided by iem-database test database."""
    for _ in range(2):
        req = client.get("/mos.json?station=KDSM&model=NBS")
        assert req.status_code == 200


def test_basic():
    """Test."""
    req = client.get(
        "/mos.json?station=KDSM&model=GFS&runtime=2024-08-02T00:00:00Z"
    )
    assert req.status_code == 200


def test_bad_model():
    """Test with bad model."""
    req = client.get("/mos.json?station=KDSM&model=xxx")
    assert req.status_code == 422
