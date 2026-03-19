from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from app.observability import REQUEST_ID_HEADER, get_request_id
from app.schemas import SpreadsheetChatRequest
from app.services.spreadsheet.service import stream_spreadsheet_chat
from app.services.storage import file_storage


router = APIRouter()


@router.post("/spreadsheet/chat/stream")
async def spreadsheet_chat_stream(request: SpreadsheetChatRequest, http_request: Request) -> StreamingResponse:
    request_id = get_request_id(http_request)
    stored = file_storage.get(request.file_id)
    stream = stream_spreadsheet_chat(
        path=stored.path,
        file_id=request.file_id,
        chat_text=request.chat_text,
        mode=request.mode,
        sheet_index=request.sheet_index,
        sheet_override=request.sheet_override,
        locale=request.locale,
        conversation_id=request.conversation_id,
        clarification_resolution=request.clarification_resolution.model_dump() if request.clarification_resolution is not None else None,
        request_id=request_id,
    )
    return StreamingResponse(stream, media_type="text/event-stream", headers={REQUEST_ID_HEADER: request_id})
