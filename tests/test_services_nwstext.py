"""Test the nwstext service."""

from fastapi.testclient import TestClient


def test_invalid_timestamp(client: TestClient):
    """Test that this raises a 422."""
    req = client.get("/nwstext/20150705-0517-KOKX-CDUS41-CLINYC")
    assert req.status_code == 422


def test_invalid_request(client: TestClient):
    """Test basic things."""
    req = client.get("/nwstext/BAH")
    assert req.status_code == 422


def test_valid_request(client: TestClient):
    """Test valid request."""
    pid = "202101010000-KDMX-TTAAII-AAABBB"
    req = client.get(f"/nwstext/{pid}")
    assert "X-IEM-Notice" in req.headers
    assert req.text == "HI DARYL, NULL BBB"


def test_nolimit(client: TestClient):
    """Test a nolimit request."""
    pid = "202101010000-KDMX-TTAAII-AAABBB"
    req = client.get(f"/nwstext/{pid}?nolimit=1")
    assert "X-IEM-Notice" in req.headers
    assert req.text.find("\003") > 0


def test_valid_request_bbb(client: TestClient):
    """Test valid request."""
    pid = "202101010000-KDMX-TTAAII-AAABBB-RRA"
    req = client.get(f"/nwstext/{pid}")
    assert req.text == "HI DARYL"
