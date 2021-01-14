"""Test idot_dashcam."""
import os

from fastapi.testclient import TestClient
import pytest
from iemws.main import app

client = TestClient(app)


@pytest.mark.skipif(os.environ.get("HAS_IEMDATABASE") == "0", reason="NODB")
def test_first():
    """Test we can do things."""
    res = client.get(
        "/idot_dashcam.geojson", params={"valid": "2021-01-01T12:00Z"}
    )
    j = res.json()
    assert "features" in j


@pytest.mark.skipif(os.environ.get("HAS_IEMDATABASE") == "0", reason="NODB")
def test_json():
    """Test we can do things."""
    res = client.get(
        "/idot_dashcam.json", params={"valid": "2021-01-01T12:00"}
    )
    j = res.json()
    assert "data" in j
