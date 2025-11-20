"""Test the mos service."""

# third party
from fastapi.testclient import TestClient

from iemws.main import app

client = TestClient(app)


def test_malformed_station():
    """Test that we get a 422 when provided a poorly formed station."""
    resp = client.get("/mos.json?station=KS&model=GFS")
    assert resp.status_code == 422


def test_faked_nbs():
    """Test data provided by iem-database test database."""
    # Note there is a bootstrap in CI that generates some near realtime data
    for _ in range(2):
        resp = client.get("/mos.json?station=KDSM&model=NBS")
        assert resp.status_code == 200


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


def test_no_recent():
    """Test with bad model."""
    req = client.get("/mos.json?station=KXX1&model=NAM")
    assert req.status_code == 404
