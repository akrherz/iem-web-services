"""Test the nws/bufkit service."""
# stdlib
import re

# third party
import pytest
from fastapi.testclient import TestClient

# Local
from iemws.main import app
from iemws.services.nws import bufkit

URLS = re.compile(r"`/api/1([^\s]*)`")
client = TestClient(app)


def test_basic():
    """Test basic calls."""
    req = client.get("/nws/bufkit.json?lon=-92.5&lat=42.5")
    res = req.json()
    assert res is not None


@pytest.mark.parametrize("url", URLS.findall(bufkit.__doc__))
def test_docustring(url):
    """Test example URLs found in the docstring."""
    res = client.get(url).json()
    assert res is not None


def test_bad_model():
    """Test that an error comes for a bad model."""
    res = client.get("/nws/bufkit.json?model=Q")
    assert res.status_code == 500


def test_setting_runtime_but_no_runtime():
    """Test this combo."""
    res = client.get("/nws/bufkit.json?runtime=2021-01-01T00:00&station=KDSM")
    assert res.status_code == 500


def test_nam4km():
    """Test that the NAM4KM returns content."""
    res = client.get(
        "/nws/bufkit.json?time=2021-01-01T01:00&station=KDSM&model=NAM4KM"
    )
    assert res.status_code == 200
