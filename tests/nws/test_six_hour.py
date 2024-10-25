"""Test the nws/six_hour service."""

from fastapi.testclient import TestClient

from iemws.main import app

client = TestClient(app)


def test_basic():
    """Test basic calls."""
    for varname in "swe snowfall precip snowdepth".split():
        service = f"/nws/{varname}_6hour.geojson?valid=2023-11-10T12:00:00Z"
        res = client.get(service).json()
        assert res["features"][0]["properties"]["station"] == "DNKI4"
