"""Test the nwstext service."""

from fastapi.testclient import TestClient
from iemws.main import app

client = TestClient(app)


def test_invalid_request():
    """Test basic things."""
    req = client.get("/nwstext/BAH")
    assert req.status_code == 422


def test_valid_request():
    """Test valid request."""
    pid = "202101010000-KDMX-TTAAII-AAABBB"
    req = client.get(f"/nwstext/{pid}")
    assert req.text == "HI DARYL"


def test_valid_request_bbb():
    """Test valid request."""
    pid = "202101010000-KDMX-TTAAII-AAABBB-RRA"
    req = client.get(f"/nwstext/{pid}")
    assert req.text == "HI DARYL"
