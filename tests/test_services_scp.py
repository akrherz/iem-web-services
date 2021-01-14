"""Test SCP."""

# Third Party
from fastapi.testclient import TestClient

# Local
from iemws.main import app

client = TestClient(app)


def test_first():
    """Test we can do things."""
    res = client.get(
        "/scp.json", params={"station": "KAMW", "date": "2020-10-26"}
    )
    j = res.json()
    assert "data" in j
