from __future__ import annotations

from app.config import get_settings

from .base import FileStore, StoredFile
from .local_file_store import LocalFileStore
from .object_storage_file_store import ObjectStorageFileStore


def build_file_store() -> FileStore:
    settings = get_settings()
    backend = str(settings.file_store_backend or "local").strip().lower()
    if backend == "local":
        return LocalFileStore()
    if backend in {"object", "s3", "oss"}:
        return ObjectStorageFileStore()
    raise RuntimeError(f"Unsupported file store backend: {backend}")


file_storage = build_file_store()

__all__ = [
    "FileStore",
    "LocalFileStore",
    "ObjectStorageFileStore",
    "StoredFile",
    "build_file_store",
    "file_storage",
]
