"""Tests for the custom Gunicorn worker."""

from iemws.worker import RestartableUvicornWorker


def test_worker_configures_api_root_path():
    """Configure the production API prefix in Uvicorn."""
    assert RestartableUvicornWorker.CONFIG_KWARGS["root_path"] == "/api/1"
