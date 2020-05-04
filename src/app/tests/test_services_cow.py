"""These are slower tests, so they go here."""
import os

from fastapi.testclient import TestClient
import pytest

from ..main import app

client = TestClient(app)


@pytest.mark.skipif(os.environ.get("NODATABASE") == "1", reason="NODB")
def test_issue10_nowfo():
    """Test that we need not provide a WFO."""
    res = client.get(
        "/cow.json",
        params={"begints": "2020-05-03T12:00Z", "endts": "2020-05-04T12:00Z"},
    )
    cow = res.json()
    assert cow["stats"]["events_total"] == 180


@pytest.mark.skipif(os.environ.get("NODATABASE") == "1", reason="NODB")
def test_iemissue163_slowlix():
    """See why this query is so slow!"""
    res = client.get(
        "/cow.json",
        params={
            "wfo": "LIX",
            "begints": "2018-01-01T12:00Z",
            "endts": "2018-07-16T12:00Z",
        },
    )
    cow = res.json()
    assert cow["stats"]["events_total"] == 395


@pytest.mark.skipif(os.environ.get("NODATABASE") == "1", reason="NODB")
def test_dsw():
    """Dust Storm Warnings"""
    params = {
        "wfo": "PSR",
        "phenomena": "DS",
        "lsrtype": "DS",
        "begints": "2018-07-01T12:00Z",
        "endts": "2018-07-10T12:00Z",
        "hailsize": 1.0,
    }
    res = client.get("/cow.json", params=params)
    cow = res.json()
    assert cow["stats"]["events_total"] == 18


@pytest.mark.skipif(os.environ.get("NODATABASE") == "1", reason="NODB")
def test_190806():
    """Test that we can limit verification to tags."""

    params = {
        "wfo": "DMX",
        "begints": "2018-06-20T12:00Z",
        "endts": "2018-06-30T12:00Z",
        "hailsize": 1.0,
    }
    res = client.get("/cow.json", params=params)
    cow = res.json()
    assert cow["stats"]["warned_reports"] == 56
    params["windhailtag"] = "Y"
    res = client.get("/cow.json", params=params)
    cow = res.json()
    assert cow["stats"]["warned_reports"] == 46


@pytest.mark.skipif(os.environ.get("NODATABASE") == "1", reason="NODB")
def test_180620():
    """Compare with what we have from legacy PHP based Cow"""
    params = {
        "wfo": "DMX",
        "begints": "2018-06-20T12:00Z",
        "endts": "2018-06-21T12:00Z",
        "hailsize": 1.0,
    }
    res = client.get("/cow.json", params=params)
    cow = res.json()
    assert cow["stats"]["events_total"] == 18
    assert cow["stats"]["events_verified"] == 4
    assert abs(cow["stats"]["size_poly_vs_county[%]"] - 13.3) < 0.1
    assert abs(cow["stats"]["area_verify[%]"] - 17.0) < 0.1
    # TODO
    # _ev = cow.events.iloc[0]
    # assert abs(_ev["parea"] - 919.0) < 1
    # assert abs(_ev["parea"] / _ev["carea"] - 0.19) < 0.01


@pytest.mark.skipif(os.environ.get("NODATABASE") == "1", reason="NODB")
def test_one():
    """Compare with what we have from legacy PHP based Cow"""
    params = {
        "wfo": "DMX",
        "begints": "2018-06-18T12:00Z",
        "endts": "2018-06-20T12:00Z",
        "hailsize": 1.0,
    }
    res = client.get("/cow.json", params=params)
    cow = res.json()
    assert cow["stats"]["events_total"] == 5
    assert cow["stats"]["events_verified"] == 2
    assert abs(cow["stats"]["size_poly_vs_county[%]"] - 24.3) < 0.1
    assert abs(cow["stats"]["area_verify[%]"] - 15.2) < 0.1
    # TODO
    # _ev = cow.events.iloc[0]
    # assert abs(_ev["parea"] - 950.0) < 1
    # assert abs(_ev["parea"] / _ev["carea"] - 0.159) < 0.01
