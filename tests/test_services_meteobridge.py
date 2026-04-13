"""Test the networks service."""

from fastapi.testclient import TestClient

from iemws.services.meteobridge import PROPS


def test_invalid_apikey(client: TestClient):
    """Test invalid API key."""
    PROPS.clear()
    resp = client.get(
        "/meteobridge.json?key=badkey&time=20260412050452&sknt=0.8&tmpf=70.88&"
        "max_tmpf=86.90&min_tmpf=65.30&relh=17.00&dwpf=23.90&drct=251.00&"
        "pday=0.00&alti=29.82"
    )
    assert resp.status_code == 404


def test_basic(client: TestClient):
    """Test basic things."""
    PROPS["meteobridge.key.OT0013"] = "999"
    resp = client.get(
        "/meteobridge.json?key=999&time=20260412050452&sknt=0.8&tmpf=70.88&"
        "max_tmpf=86.90&min_tmpf=65.30&relh=17.00&dwpf=23.90&drct=251.00&"
        "pday=0.00&alti=29.82"
    )
    data = resp.json()
    assert data == "OK"
    assert resp.status_code == 200
