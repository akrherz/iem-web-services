"""Unused at the moment."""
import os

from fastapi.testclient import TestClient

from ..main import app

client = TestClient(app)


def _test_iemissue163_slowlix():
    """See why this query is so slow!"""
    from paste.util.multidict import MultiDict

    flds = MultiDict()
    flds.add("wfo", "LIX")
    flds.add("begints", "2018-01-01T12:00")
    flds.add("endts", "2018-07-16T12:00")
    cow = COWSession(flds)
    cow.milk()
    assert cow.stats["events_total"] == 395


def test_currents():
    """Test we can get currents."""
    res = client.get("/currents.json", params={"network": "IA_ASOS"})
    answer = 503 if os.environ.get("NODATABASE", "0") == "1" else 200
    assert res.status_code == answer


def test_empty():
    """Can we run when no data is found?"""
    response = client.get(
        "/cow.json",
        params={
            "wfo": "DMX",
            "begints": "2000-01-01T12:00Z",
            "endts": "2000-01-02T12:00Z",
        },
    )
    answer = 503 if os.environ.get("NODATABASE", "0") == "1" else 200
    assert response.status_code == answer


def _test_dsw():
    """Dust Storm Warnings"""
    from paste.util.multidict import MultiDict

    flds = MultiDict()
    flds.add("wfo", "PSR")
    flds.add("phenomena", "DS")
    flds.add("lsrtype", "DS")
    flds.add("begints", "2018-07-01T12:00")
    flds.add("endts", "2018-07-10T12:00")
    flds.add("hailsize", 1.0)
    cow = COWSession(flds)
    cow.milk()
    assert cow.stats["events_total"] == 18


def _test_190806():
    """Test that we can limit verification to tags."""
    from paste.util.multidict import MultiDict

    flds = MultiDict()
    flds.add("wfo", "DMX")
    flds.add("begints", "2018-06-20T12:00")
    flds.add("endts", "2018-06-30T12:00")
    flds.add("hailsize", 1.0)
    cow = COWSession(flds)
    cow.milk()
    assert cow.stats["warned_reports"] == 56
    flds.add("windhailtag", "Y")
    cow2 = COWSession(flds)
    cow2.milk()
    assert cow2.stats["warned_reports"] == 46


def _test_180620():
    """Compare with what we have from legacy PHP based Cow"""
    from paste.util.multidict import MultiDict

    flds = MultiDict()
    flds.add("wfo", "DMX")
    flds.add("begints", "2018-06-20T12:00")
    flds.add("endts", "2018-06-21T12:00")
    flds.add("hailsize", 1.0)
    cow = COWSession(flds)
    cow.milk()
    assert cow.stats["events_total"] == 18
    assert cow.stats["events_verified"] == 4
    assert abs(cow.stats["size_poly_vs_county[%]"] - 13.3) < 0.1
    assert abs(cow.stats["area_verify[%]"] - 17.0) < 0.1
    _ev = cow.events.iloc[0]
    assert abs(_ev["parea"] - 919.0) < 1
    assert abs(_ev["parea"] / _ev["carea"] - 0.19) < 0.01


def _test_one():
    """Compare with what we have from legacy PHP based Cow"""
    from paste.util.multidict import MultiDict

    flds = MultiDict()
    flds.add("wfo", "DMX")
    flds.add("begints", "2018-06-18T12:00")
    flds.add("endts", "2018-06-20T12:00")
    flds.add("hailsize", 1.0)
    cow = COWSession("DMX")
    cow.milk()
    assert cow.stats["events_total"] == 5
    assert cow.stats["events_verified"] == 2
    assert abs(cow.stats["size_poly_vs_county[%]"] - 24.3) < 0.1
    assert abs(cow.stats["area_verify[%]"] - 15.2) < 0.1
    _ev = cow.events.iloc[0]
    assert abs(_ev["parea"] - 950.0) < 1
    assert abs(_ev["parea"] / _ev["carea"] - 0.159) < 0.01
