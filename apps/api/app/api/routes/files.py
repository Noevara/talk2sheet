from __future__ import annotations

from pathlib import Path
from time import perf_counter

from fastapi import APIRouter, File, HTTPException, Request, UploadFile

from app.observability import elapsed_ms, get_logger, get_request_id, log_request_event
from app.schemas import PreviewResponse, UploadedFileResponse
from app.services.spreadsheet.pipeline import SUPPORTED_SPREADSHEET_SUFFIXES, preview_sheet, read_sheet_descriptors
from app.services.storage import file_storage


router = APIRouter()
logger = get_logger(__name__)


@router.post("/files/upload", response_model=UploadedFileResponse)
async def upload_file(request: Request, file: UploadFile = File(...)) -> UploadedFileResponse:
    request_id = get_request_id(request)
    if not file.filename:
        raise HTTPException(status_code=400, detail="Missing file name.")
    suffix = Path(file.filename).suffix.lower()
    if suffix not in SUPPORTED_SPREADSHEET_SUFFIXES:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type: {suffix or 'unknown'}. Supported types: .xlsx, .xls, .csv.",
        )
    started_at = perf_counter()
    log_request_event(
        logger,
        "file_upload_started",
        request_id,
        filename=file.filename,
        suffix=suffix,
    )
    stored = await file_storage.save_upload(file)
    sheets = read_sheet_descriptors(stored.path)
    log_request_event(
        logger,
        "file_upload_completed",
        request_id,
        file_id=stored.file_id,
        file_type=stored.file_type,
        sheet_count=len(sheets),
        duration_ms=elapsed_ms(started_at),
    )
    return UploadedFileResponse(
        file_id=stored.file_id,
        file_name=stored.file_name,
        file_type=stored.file_type,
        sheets=sheets,
    )


@router.get("/files/{file_id}/sheets", response_model=UploadedFileResponse)
def list_sheets(file_id: str, request: Request) -> UploadedFileResponse:
    request_id = get_request_id(request)
    started_at = perf_counter()
    stored = file_storage.get(file_id)
    sheets = read_sheet_descriptors(stored.path)
    log_request_event(
        logger,
        "file_sheets_listed",
        request_id,
        file_id=file_id,
        sheet_count=len(sheets),
        duration_ms=elapsed_ms(started_at),
    )
    return UploadedFileResponse(
        file_id=stored.file_id,
        file_name=stored.file_name,
        file_type=stored.file_type,
        sheets=sheets,
    )


@router.get("/files/{file_id}/preview", response_model=PreviewResponse)
def get_preview(file_id: str, request: Request, sheet_index: int = 1) -> PreviewResponse:
    request_id = get_request_id(request)
    started_at = perf_counter()
    stored = file_storage.get(file_id)
    preview = preview_sheet(stored.path, file_id=file_id, sheet_index=sheet_index)
    log_request_event(
        logger,
        "file_preview_loaded",
        request_id,
        file_id=file_id,
        sheet_index=sheet_index,
        row_count=len(preview.rows),
        column_count=len(preview.columns),
        duration_ms=elapsed_ms(started_at),
    )
    return preview
