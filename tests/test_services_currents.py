"""Test the currents service."""

from fastapi.testclient import TestClient


def test_networkclass_and_wfo(client: TestClient):
    """Test this combination."""
    resp = client.get("/currents.json?networkclass=ASOS&wfo=KDMX")
    assert resp.status_code == 200


def test_state(client: TestClient):
    """Test conventional state query."""
    resp = client.get("/currents.json?state=IA")
    assert resp.status_code == 200


def test_network(client: TestClient):
    """Test conventional network query."""
    resp = client.get("/currents.json?network=IA_COOP")
    assert resp.status_code == 200


def test_wfo(client: TestClient):
    """Test conventional WFO query."""
    resp = client.get("/currents.json?wfo=DMX")
    assert resp.status_code == 200


def test_cache_buster_422s(client: TestClient):
    """Test that we don't permit extra args."""
    resp = client.get("/currents.json?network=CCOOP&_cb=1")
    assert resp.status_code == 422


def test_ccoop(client: TestClient):
    """Exercise this special selector."""
    resp = client.get("/currents.json?network=CCOOP")
    assert resp.status_code == 200


def test_station(client: TestClient):
    """Test that we can query by station."""
    resp = client.get("/currents.json?station=AMW")
    res = resp.json()
    assert res is not None


def test_basic(client: TestClient):
    """Test that we need not provide a WFO."""
    resp = client.get("/currents.json")
    res = resp.json()
    assert res is not None


def test_issue61_uscurrents(client: TestClient):
    """Test that we can query by networkclass and country."""
    resp = client.get("/currents.txt?networkclass=ASOS&country=US")
    assert resp.text is not None
