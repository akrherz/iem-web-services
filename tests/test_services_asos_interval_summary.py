"""Test the asos_interval_summary service."""

from fastapi.testclient import TestClient

from iemws.main import app

client = TestClient(app)


def test_nodata():
    """Test a 404."""
    resp = client.get(
        "/asos_interval_summary.json?"
        "station=_X_X&sts=2020-08-01T00:00&ets=2020-08-02T00:00"
    )
    assert resp.status_code == 404


def test_gh217_wind_fields():
    """Test that wind fields are properly provided."""
    resp = client.get(
        "/asos_interval_summary.json?"
        "station=DSM&sts=2020-01-10T23:00&ets=2020-01-12T04:00"
    )
    jdata = resp.json()
    assert jdata["data"][0]["max_speed_kts"] == 21
    assert jdata["data"][0]["max_gust_kts"] == 34
    assert jdata["data"][0]["max_speed_drct"] == 350
    assert jdata["data"][0]["max_gust_drct"] == 360
    assert jdata["data"][0]["max_speed_time_utc"] == "2020-01-11T02:35:00Z"
    assert jdata["data"][0]["max_gust_time_utc"] == "2020-01-11T01:14:00Z"


def test_logic():
    """Test something with data."""
    resp = client.get(
        "/asos_interval_summary.json?"
        "station=DSM&sts=2020-08-01T00:00&ets=2020-08-02T00:00"
    )
    jdata = resp.json()
    assert jdata["data"][0]["max_tmpf"] == 84
    assert jdata["data"][0]["min_tmpf"] == 61


def test_logic_excluding_6hr():
    """This request should exclude the 6 hour value at 0z."""
    resp = client.get(
        "/asos_interval_summary.json?"
        "station=DSM&sts=2020-08-01T22:00&ets=2020-08-02T00:00"
    )
    jdata = resp.json()
    assert jdata["data"][0]["max_tmpf"] == 83
    assert jdata["data"][0]["min_tmpf"] == 79


def test_logic_precip():
    """Test that we get precipitation right."""
    resp = client.get(
        "/asos_interval_summary.json?"
        "station=DSM&sts=2020-01-11T03:00&ets=2020-01-11T06:00"
    )
    jdata = resp.json()
    assert abs(jdata["data"][0]["total_precip_in"] - 0.01) < 0.00001


def test_logic_precip_multistations():
    """Test that we get precipitation right."""
    resp = client.get(
        "/asos_interval_summary.json?"
        "station=DSM,AMW&sts=2020-01-11T03:00&ets=2020-01-11T06:00"
    )
    jdata = resp.json()
    assert abs(jdata["data"][0]["total_precip_in"] - 0.01) < 0.00001
    assert jdata["data"][1]["total_precip_in"] == 0


def test_comparison_issue():
    """Test something found whilst manual testing."""
    resp = client.get(
        "/asos_interval_summary.json?station=DSM,Z00&"
        "sts=2020-07-01T12:00:00Z&ets=2020-07-10T12:00:00Z"
    )
    assert resp.status_code == 200


def test_too_large_request():
    """Don't allow such a large request."""
    resp = client.get(
        "/asos_interval_summary.json?station=DSM,AMW&"
        "sts=2019-07-01T12:00:00Z&ets=2020-07-10T12:00:00Z"
    )
    assert resp.status_code == 400
