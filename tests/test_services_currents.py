"""Test the currents service."""

from fastapi.testclient import TestClient

from iemws.main import app

client = TestClient(app)


def test_basic():
    """Test that we need not provide a WFO."""
    req = client.get("/currents.json")
    res = req.json()
    assert res is not None


def test_issue61_uscurrents():
    """Test that we can query by networkclass and country."""
    req = client.get("/currents.txt?networkclass=ASOS&country=US")
    assert req.text is not None
