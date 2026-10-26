"""Tests for application behavior behind the API prefix proxy."""

from fastapi.testclient import TestClient

from iemws.main import app


def test_docs_include_proxy_root_path():
    """Swagger UI requests its OpenAPI schema under the proxy prefix."""
    response = TestClient(app, root_path="/api/1").get("/docs")

    assert response.status_code == 200
    assert "url: '/api/1/openapi.json'" in response.text
