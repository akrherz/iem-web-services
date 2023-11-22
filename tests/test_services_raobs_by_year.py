"""Test raobs_by_year."""

# Third Party
from fastapi.testclient import TestClient

# Local
from iemws.main import app

client = TestClient(app)


def test_first():
    """Test we can do things."""
    res = client.get(
        "/raobs_by_year.json", params={"station": "_OAX", "year": 2023}
    )
    j = res.json()
    assert "data" in j
