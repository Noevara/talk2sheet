from __future__ import annotations

from fastapi import HTTPException, UploadFile

from .base import FileStore, StoredFile


class ObjectStorageFileStore(FileStore):
    async def save_upload(self, file: UploadFile) -> StoredFile:
        raise HTTPException(status_code=501, detail="Object storage file store is not implemented yet.")

    def get(self, file_id: str) -> StoredFile:
        raise HTTPException(status_code=501, detail="Object storage file store is not implemented yet.")

    def delete(self, file_id: str) -> None:
        raise HTTPException(status_code=501, detail="Object storage file store is not implemented yet.")
