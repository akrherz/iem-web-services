"""Test the nws/afos/list service."""

from fastapi.testclient import TestClient
from iemws.main import app

client = TestClient(app)


def test_char3_source():
    """Test that we can deal with a 3 char source."""
    req = client.get("/nws/afos/list.json?cccc=DMX&date=2022-10-24")
    res = req.json()
    assert res is not None


def test_badcall():
    """Test what happens when we don't set anything."""
    req = client.get("/nws/afos/list.json")
    assert req.status_code == 400


def test_basic():
    """Test basic calls."""
    req = client.get("/nws/afos/list.json?cccc=KWNO")
    res = req.json()
    assert res is not None


def test_both():
    """Test specifying both."""
    lst = "cccc=KDMX&pil=TORDMX pil=TORDMX cccc=KDMX&pil=TOR pil=TOR".split()
    for c in lst:
        req = client.get(f"/nws/afos/list.json?{c}")
        res = req.json()
        assert res is not None
