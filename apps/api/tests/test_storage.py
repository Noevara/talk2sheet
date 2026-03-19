from __future__ import annotations

import io
import json
from pathlib import Path

import pytest
from fastapi import HTTPException
from starlette.datastructures import UploadFile

from app.services.storage import LocalFileStore


@pytest.mark.asyncio
async def test_local_file_store_saves_upload_in_chunks(tmp_path, monkeypatch) -> None:
    import app.services.storage.local_file_store as storage_module

    class _Settings:
        upload_dir = tmp_path / "uploads"
        metadata_dir = tmp_path / "metadata"

    _Settings.upload_dir.mkdir(parents=True, exist_ok=True)
    _Settings.metadata_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(storage_module, "get_settings", lambda: _Settings)

    store = LocalFileStore(chunk_size=3)
    upload = UploadFile(filename="sample.csv", file=io.BytesIO(b"abcdefghi"))

    stored = await store.save_upload(upload)

    assert stored.file_type == "csv"
    assert stored.path.read_bytes() == b"abcdefghi"
    metadata = json.loads((_Settings.metadata_dir / f"{stored.file_id}.json").read_text(encoding="utf-8"))
    assert metadata["file_name"] == "sample.csv"
    assert upload.file.read() == b"abcdefghi"


def test_local_file_store_delete_removes_file_and_metadata(tmp_path, monkeypatch) -> None:
    import app.services.storage.local_file_store as storage_module

    class _Settings:
        upload_dir = tmp_path / "uploads"
        metadata_dir = tmp_path / "metadata"

    _Settings.upload_dir.mkdir(parents=True, exist_ok=True)
    _Settings.metadata_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(storage_module, "get_settings", lambda: _Settings)

    store = LocalFileStore()
    file_id = "file-1"
    target_path = _Settings.upload_dir / "file-1.csv"
    target_path.write_text("a,b\n1,2\n", encoding="utf-8")
    (_Settings.metadata_dir / "file-1.json").write_text(
        json.dumps(
            {
                "file_id": file_id,
                "file_name": "sample.csv",
                "file_type": "csv",
                "path": str(target_path),
            }
        ),
        encoding="utf-8",
    )

    store.delete(file_id)

    assert not target_path.exists()
    assert not (_Settings.metadata_dir / "file-1.json").exists()


def test_local_file_store_get_raises_not_found(tmp_path, monkeypatch) -> None:
    import app.services.storage.local_file_store as storage_module

    class _Settings:
        upload_dir = tmp_path / "uploads"
        metadata_dir = tmp_path / "metadata"

    _Settings.upload_dir.mkdir(parents=True, exist_ok=True)
    _Settings.metadata_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(storage_module, "get_settings", lambda: _Settings)

    store = LocalFileStore()

    with pytest.raises(HTTPException) as exc_info:
        store.get("missing")

    assert exc_info.value.status_code == 404
    assert exc_info.value.detail == "Spreadsheet file not found."
