"""Test the nws/six_hour service."""

from fastapi.testclient import TestClient

from iemws.main import app

client = TestClient(app)


def test_invalid_varname():
    """Test that we get a 422 when given a bad varname."""
    res = client.get("/nws/badvar_6hour.geojson?valid=2023-11-10T12:00:00Z")
    assert res.status_code == 422


def test_basic():
    """Test basic calls."""
    for varname in "swe snowfall precip snowdepth".split():
        service = f"/nws/{varname}_6hour.geojson?valid=2023-11-10T12:00:00Z"
        res = client.get(service).json()
        assert res["features"][0]["properties"]["station"] == "DNKI4"


def test_wfo_limit():
    """Test calling for a given WFO."""
    service = "/nws/snowfall_6hour.geojson?valid=2023-11-10T12:00:00Z&wfo=DVN"
    res = client.get(service)
    assert res.status_code == 200
