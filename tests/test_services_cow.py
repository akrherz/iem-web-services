"""These are slower tests, so they go here."""

# third party
from fastapi.testclient import TestClient

# local
from iemws.main import app

client = TestClient(app)


def test_gh163_snow_squall():
    """Test that we can exercise snow squall warnings."""
    params = {
        "begints": "2023-11-28T00:00Z",
        "endts": "2023-11-29T00:00Z",
        "wfo": "BGM",
        "phenomena": ["SQ"],
    }
    res = client.get(
        "/cow.json",
        params=params,
    )
    cow = res.json()
    assert cow["stats"]["events_total"] == 2
    assert cow["stats"]["events_verified"] == 1


def test_testdata():
    """Test what our testdata provides CI."""
    # NOTE, this is all set to Story county due to test data with 1 UGC
    params = {
        "begints": "2018-06-20T17:00Z",
        "endts": "2018-06-21T03:00Z",
        "wfo": "DMX",
        "phenomena": ["SV", "TO", "FF"],
    }
    res = client.get(
        "/cow.json",
        params=params,
    )
    cow = res.json()
    assert cow["stats"]["events_total"] == 15
    assert len(cow["stormreports"]["features"]) == 14


def test_fcster():
    """Test calling with the fcster tag."""
    params = {
        "begints": "2020-05-03T12:00Z",
        "endts": "2020-05-04T12:00Z",
        "fcster": "BOGUS",
    }
    res = client.get(
        "/cow.json",
        params=params,
    )
    cow = res.json()
    assert cow["stats"]["events_total"] == 0


def test_limitwarns():
    """Test calling with the fcster tag."""
    params = {
        "limitwarns": "Y",
        "wind": "200",
        "hailsize": "10",
        "begints": "2020-05-03T12:00Z",
        "endts": "2020-05-04T12:00Z",
    }
    res = client.get(
        "/cow.json",
        params=params,
    )
    cow = res.json()
    assert cow["stats"]["events_total"] == 0


def test_issue10_nowfo():
    """Test that we need not provide a WFO."""
    res = client.get(
        "/cow.json",
        params={"begints": "2020-05-03T12:00Z", "endts": "2020-05-04T12:00Z"},
    )
    cow = res.json()
    assert cow["stats"]["events_total"] == 180


def test_iemissue163_slowlix(prodtest):
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
    assert cow["stats"]["events_total"] == (395 if prodtest else 0)


def test_dsw(prodtest):
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
    assert cow["stats"]["events_total"] == (18 if prodtest else 0)


def test_190806(prodtest):
    """Test that we can limit verification to tags."""

    params = {
        "wfo": "DMX",
        "begints": "2012-06-20T12:00Z",
        "endts": "2012-06-30T12:00Z",
        "hailsize": 1.0,
    }
    res = client.get("/cow.json", params=params)
    cow = res.json()
    assert cow["stats"]["warned_reports"] == (56 if prodtest else 0)
    params["windhailtag"] = "Y"
    res = client.get("/cow.json", params=params)
    cow = res.json()
    assert cow["stats"]["warned_reports"] == (46 if prodtest else 0)


def test_180620(prodtest):
    """Compare with what we have from legacy PHP based Cow"""
    params = {
        "wfo": ["DMX", "XXX"],
        "begints": "2018-06-20T12:00Z",
        "endts": "2018-06-21T12:00Z",
        "hailsize": 1.0,
    }
    res = client.get("/cow.json", params=params)
    cow = res.json()
    if not prodtest:
        return
    assert cow["stats"]["events_total"] == 18
    assert cow["stats"]["events_verified"] == 4
    assert abs(cow["stats"]["area_verify[%]"] - 17.0) < 0.1
    _ev = cow["events"]["features"][0]["properties"]
    assert abs(_ev["parea"] - 919.0) < 1


def test_gh131_tornado_possible():
    """Tornado Possible in SVRs."""
    params = {
        "wfo": "DMX",
        "begints": "2024-05-21T21:00Z",
        "endts": "2024-05-21T23:00Z",
        "phenomena": "SV",
        "lsrtype": "TO",
    }
    cow = client.get("/cow.json", params=params).json()
    assert cow["stats"]["svr_with_torpossible_total"] == 2
    assert cow["stats"]["svr_with_torpossible_verified"] == 2
