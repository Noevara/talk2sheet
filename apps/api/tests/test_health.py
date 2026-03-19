from __future__ import annotations

from fastapi.testclient import TestClient

from app.main import app


client = TestClient(app)


def test_health() -> None:
    response = client.get("/api/health")
    assert response.status_code == 200
    assert response.headers["x-request-id"]
    payload = response.json()
    assert payload["status"] == "ok"


def test_missing_file_error_includes_request_id() -> None:
    response = client.get("/api/files/missing-file-id/preview")
    assert response.status_code == 404
    assert response.headers["x-request-id"]
    payload = response.json()
    assert payload["detail"] == "Spreadsheet file not found."
    assert payload["request_id"] == response.headers["x-request-id"]
