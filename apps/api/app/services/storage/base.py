from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from fastapi import UploadFile


@dataclass
class StoredFile:
    file_id: str
    file_name: str
    file_type: str
    path: Path


class FileStore(Protocol):
    async def save_upload(self, file: UploadFile) -> StoredFile:
        ...

    def get(self, file_id: str) -> StoredFile:
        ...

    def delete(self, file_id: str) -> None:
        ...
