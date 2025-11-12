"""Test the nws/bufkit service."""

import re
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient
from pytest_httpx import HTTPXMock

from iemws.main import app
from iemws.services.nws import bufkit

URLS = re.compile(r"`/api/1([^\s]*)`")
client = TestClient(app)


def test_error_handling(httpx_mock: HTTPXMock):
    """Test error handling code paths."""
    # Simulate a timeout
    httpx_mock.add_exception(
        httpx.TimeoutException("Simulated timeout for testing")
    )
    res = client.get("/nws/bufkit.json?lon=-92.5&lat=42.5")
    assert res.status_code == 503
    assert "mtarchive backend failed" in res.json().get("detail", "").lower()


def test_error_with_invalid_file(httpx_mock: HTTPXMock):
    """Test when the service provides an empty file."""
    testfn = Path(__file__).parent / "rap_kdsm.buf"
    with open(testfn) as fh:
        httpx_mock.add_response(content=fh.read(1000).encode())
    res = client.get("/nws/bufkit.json?lon=-92.5&lat=42.5")
    assert res.status_code == 503
    assert "raw bufkit file failed" in res.json().get("detail", "").lower()


def test_230220_multiple_stations():
    """Test we don't get a traceback."""
    req = client.get("nws/bufkit.txt?model=GFS&fall=1&station=KFMH")
    assert req.status_code in [200, 503]


def test_basic():
    """Test basic calls."""
    req = client.get("/nws/bufkit.json?lon=-92.5&lat=42.5")
    res = req.json()
    assert res is not None


@pytest.mark.parametrize("url", URLS.findall(str(bufkit.__doc__)))
def test_docustring(url):
    """Test example URLs found in the docstring."""
    res = client.get(url).json()
    assert res is not None


def test_bad_model():
    """Test that an error comes for a bad model."""
    res = client.get("/nws/bufkit.json?model=Q")
    assert res.status_code in [422, 503]


def test_setting_runtime_but_no_runtime():
    """Test this combo."""
    res = client.get("/nws/bufkit.json?runtime=2021-01-01T00:00&station=KDSM")
    assert res.status_code in [422, 503]


def test_nam4km():
    """Test that the NAM4KM returns content."""
    res = client.get(
        "/nws/bufkit.json?time=2021-01-01T01:00&station=KDSM&model=NAM4KM"
    )
    assert res.status_code in [200, 503]


def test_nam4km_threechar():
    """Test that the NAM4KM returns content."""
    res = client.get(
        "/nws/bufkit.json?time=2021-01-01T01:00&station=DSM&model=NAM4KM"
    )
    assert res.status_code in [200, 503]


def test_gr():
    """Test the GR flag."""
    res = client.get(
        "/nws/bufkit.json?time=2021-01-01T01:00&station=KDSM&gr=1"
    )
    assert res.status_code in [200, 503]


def test_210311_gfs():
    """Test a failure seen with GFS."""
    res = client.get(
        "/nws/bufkit.json?runtime=2021-03-11T00:00&station=KDEN&fall=1&"
        "model=GFS"
    )
    assert res.status_code in [200, 503]
