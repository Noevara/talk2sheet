from __future__ import annotations

import io
import json
import logging
from pathlib import Path

from fastapi.testclient import TestClient

from app.main import app


client = TestClient(app)


def test_custom_request_id_is_echoed_by_http_middleware() -> None:
    response = client.get("/api/health", headers={"X-Request-ID": "req-custom-health"})

    assert response.status_code == 200
    assert response.headers["x-request-id"] == "req-custom-health"


def test_upload_route_logs_request_lifecycle(monkeypatch, caplog, tmp_path: Path) -> None:
    import app.api.routes.files as files_route_module

    class _StoredFile:
        file_id = "file-log-1"
        file_name = "demo.csv"
        file_type = "csv"
        path = tmp_path / "demo.csv"

    _StoredFile.path.write_text("a,b\n1,2\n", encoding="utf-8")

    async def _save_upload(_file):
        return _StoredFile

    monkeypatch.setattr(files_route_module.file_storage, "save_upload", _save_upload)
    monkeypatch.setattr(files_route_module, "read_sheet_descriptors", lambda _path: [])

    caplog.set_level(logging.INFO)
    response = client.post(
        "/api/files/upload",
        headers={"X-Request-ID": "req-upload-log"},
        files={"file": ("demo.csv", io.BytesIO(b"a,b\n1,2\n"), "text/csv")},
    )

    assert response.status_code == 200

    messages = [json.loads(record.message) for record in caplog.records if record.message.startswith("{")]
    assert any(message.get("event") == "file_upload_started" and message.get("request_id") == "req-upload-log" for message in messages)
    assert any(message.get("event") == "file_upload_completed" and message.get("request_id") == "req-upload-log" for message in messages)
    assert any(message.get("event") == "http_request_completed" and message.get("request_id") == "req-upload-log" for message in messages)
