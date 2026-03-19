from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, File, HTTPException, UploadFile

from app.schemas import PreviewResponse, UploadedFileResponse
from app.services.spreadsheet.pipeline import SUPPORTED_SPREADSHEET_SUFFIXES, preview_sheet, read_sheet_descriptors
from app.services.storage import file_storage


router = APIRouter()


@router.post("/files/upload", response_model=UploadedFileResponse)
async def upload_file(file: UploadFile = File(...)) -> UploadedFileResponse:
    if not file.filename:
        raise HTTPException(status_code=400, detail="Missing file name.")
    suffix = Path(file.filename).suffix.lower()
    if suffix not in SUPPORTED_SPREADSHEET_SUFFIXES:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type: {suffix or 'unknown'}. Supported types: .xlsx, .xls, .csv.",
        )
    stored = await file_storage.save_upload(file)
    sheets = read_sheet_descriptors(stored.path)
    return UploadedFileResponse(
        file_id=stored.file_id,
        file_name=stored.file_name,
        file_type=stored.file_type,
        sheets=sheets,
    )


@router.get("/files/{file_id}/sheets", response_model=UploadedFileResponse)
def list_sheets(file_id: str) -> UploadedFileResponse:
    stored = file_storage.get(file_id)
    sheets = read_sheet_descriptors(stored.path)
    return UploadedFileResponse(
        file_id=stored.file_id,
        file_name=stored.file_name,
        file_type=stored.file_type,
        sheets=sheets,
    )


@router.get("/files/{file_id}/preview", response_model=PreviewResponse)
def get_preview(file_id: str, sheet_index: int = 1) -> PreviewResponse:
    stored = file_storage.get(file_id)
    return preview_sheet(stored.path, file_id=file_id, sheet_index=sheet_index)
