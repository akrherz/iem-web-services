"""Test the drydown service."""

from fastapi.testclient import TestClient

# These are provided by the test data.
TESTDB_HAS_LON = -96.0
TESTDB_HAS_LAT = 43.0


def test_sday_after_eday(client: TestClient):
    """Test that sday makes sense."""
    resp = client.get("/drydown.json?sday=0601&eday=0501&lat=42.2&lon=-95.2")
    assert resp.status_code == 400
    assert resp.json()["detail"].startswith("Start date")


def test_nodata_found(client: TestClient):
    """Test something with no data."""
    resp = client.get("/drydown.json?lat=24.4&lon=-84.5&sday=0101&eday=1231")
    assert resp.status_code == 404
    assert resp.json()["detail"].startswith("No data found")


def test_out_of_iemre_bounds(client: TestClient):
    """Test something with no data."""
    resp = client.get("/drydown.json?lat=84.4&lon=-84.5&sday=0101&eday=1231")
    assert resp.status_code == 404
    assert resp.json()["detail"].startswith("Point outside of IEMRE")


def test_basic(client: TestClient):
    """Test a basic request."""
    resp = client.get(
        f"/drydown.json?lat={TESTDB_HAS_LAT}&lon={TESTDB_HAS_LON}&"
        "sday=0101&eday=1231"
    )
    res = resp.json()
    assert "data" in res
