"""Test raobs_by_year."""

# Third Party
from fastapi.testclient import TestClient

# Local
from iemws.main import app

client = TestClient(app)


def test_without_year_json():
    """Test support for no year provision."""
    res = client.get("/raobs_by_year.json", params={"station": "KOAX"})
    assert res.status_code == 422


def test_without_year_csv():
    """Test that this generated."""
    res = client.get("/raobs_by_year.txt", params={"station": "KOAX"})
    assert res.status_code == 200


def test_first():
    """Test we can do things."""
    res = client.get(
        "/raobs_by_year.json", params={"station": "_OAX", "year": 2023}
    )
    j = res.json()
    assert "data" in j
