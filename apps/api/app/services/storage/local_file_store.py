from __future__ import annotations

import json
from pathlib import Path
from uuid import uuid4

from fastapi import HTTPException, UploadFile

from app.config import get_settings

from .base import FileStore, StoredFile


DEFAULT_UPLOAD_CHUNK_SIZE = 1024 * 1024


class LocalFileStore(FileStore):
    def __init__(self, *, chunk_size: int = DEFAULT_UPLOAD_CHUNK_SIZE) -> None:
        self.settings = get_settings()
        self.chunk_size = max(1, int(chunk_size))

    def _metadata_path(self, file_id: str) -> Path:
        return self.settings.metadata_dir / f"{file_id}.json"

    def _resolve_path(self, file_id: str, suffix: str) -> Path:
        return self.settings.upload_dir / f"{file_id}{suffix}"

    async def _write_upload_chunks(self, file: UploadFile, target_path: Path) -> None:
        with target_path.open("wb") as output:
            while True:
                chunk = await file.read(self.chunk_size)
                if not chunk:
                    break
                output.write(chunk)
        await file.seek(0)

    async def save_upload(self, file: UploadFile) -> StoredFile:
        file_id = uuid4().hex
        suffix = Path(file.filename or "upload").suffix.lower()
        target_path = self._resolve_path(file_id, suffix)
        await self._write_upload_chunks(file, target_path)

        payload = {
            "file_id": file_id,
            "file_name": file.filename or target_path.name,
            "file_type": suffix.lstrip(".") or "bin",
            "path": str(target_path),
        }
        self._metadata_path(file_id).write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        return StoredFile(
            file_id=file_id,
            file_name=payload["file_name"],
            file_type=payload["file_type"],
            path=target_path,
        )

    def get(self, file_id: str) -> StoredFile:
        meta_path = self._metadata_path(file_id)
        if not meta_path.exists():
            raise HTTPException(status_code=404, detail="Spreadsheet file not found.")
        payload = json.loads(meta_path.read_text(encoding="utf-8"))
        path = Path(payload["path"])
        if not path.exists():
            raise HTTPException(status_code=404, detail="Stored spreadsheet is missing.")
        return StoredFile(
            file_id=file_id,
            file_name=payload["file_name"],
            file_type=payload["file_type"],
            path=path,
        )

    def delete(self, file_id: str) -> None:
        stored = self.get(file_id)
        if stored.path.exists():
            stored.path.unlink()
        meta_path = self._metadata_path(file_id)
        if meta_path.exists():
            meta_path.unlink()
