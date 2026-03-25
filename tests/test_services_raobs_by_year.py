"""Test raobs_by_year."""

from fastapi.testclient import TestClient


def test_unknown_virtual_station(client: TestClient):
    """Test that we get a 404 for unknown virtual station."""
    res = client.get(
        "/raobs_by_year.json",
        params={"station": "_FOO", "year": 2023},
    )
    assert res.status_code == 404


def test_invalid_sortby(client: TestClient):
    """Test that this generated."""
    res = client.get(
        "/raobs_by_year.json",
        params={"station": "KOAX", "year": 2023, "sortby": "foo"},
    )
    assert res.status_code == 422


def test_sortby(client: TestClient):
    """Test that this generated."""
    res = client.get(
        "/raobs_by_year.json",
        params={"station": "KOAX", "year": 2023, "sortby": "el_agl_m"},
    )
    assert res.status_code == 200


def test_without_year_json(client: TestClient):
    """Test support for no year provision."""
    res = client.get("/raobs_by_year.json", params={"station": "KOAX"})
    assert res.status_code == 422


def test_without_year_csv(client: TestClient):
    """Test that this generated."""
    res = client.get("/raobs_by_year.txt", params={"station": "KOAX"})
    assert res.status_code == 200


def test_first(client: TestClient):
    """Test we can do things."""
    res = client.get(
        "/raobs_by_year.json", params={"station": "_OAX", "year": 2023}
    )
    j = res.json()
    assert "data" in j
