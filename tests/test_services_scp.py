"""Test SCP."""
import os

from fastapi.testclient import TestClient
import pytest
from iemws.main import app

client = TestClient(app)


@pytest.mark.skipif(os.environ.get("HAS_IEMDATABASE") == "0", reason="NODB")
def test_first():
    """Test we can do things."""
    res = client.get(
        "/scp.json", params={"station": "KAMW", "date": "2020-10-26"}
    )
    j = res.json()
    assert j["data"]
