"""Test the drydown service."""
# local
import os
from datetime import datetime, timedelta

# third party
from fastapi.testclient import TestClient

# local
from iemws.main import app
from iemws.services.drydown import append_cfs

client = TestClient(app)


def test_append_cfs():
    """Test that we can append CFS data."""
    res = {"data": {2021: {"dates": ["2021-11-01"]}}}
    assert append_cfs(res, -95, 42) is None
    # Create an empty netcdf to exercise more API
    fn = (datetime.now() - timedelta(days=2)).strftime(
        "/mesonet/data/iemre/cfs_%Y%m%d00.nc"
    )
    should_delete = not os.path.isfile(fn)
    if should_delete:
        with open(fn, "w") as fh:
            fh.write("BAH")
    assert append_cfs(res, -95, 42) is None
    if should_delete:
        os.unlink(fn)


def test_basic():
    """Test that we need not provide a WFO."""
    req = client.get("/drydown.json")
    res = req.json()
    assert res is not None
