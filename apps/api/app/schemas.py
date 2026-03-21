from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    status: str = "ok"
    service: str = "talk2sheet-api"


class SheetDescriptor(BaseModel):
    index: int
    name: str
    rows: int | None = None
    columns: int | None = None
    field_summary: list[str] = Field(default_factory=list)


class UploadedFileResponse(BaseModel):
    file_id: str
    file_name: str
    file_type: str
    sheets: list[SheetDescriptor] = Field(default_factory=list)


class PreviewResponse(BaseModel):
    file_id: str
    sheet_index: int
    sheet_name: str
    columns: list[str]
    rows: list[list[Any]]
    total_rows: int | None = None
    preview_row_count: int | None = None


class ExecutionDisclosure(BaseModel):
    data_scope: Literal["exact_full_table", "sampled_first_n"]
    exact_used: bool
    scope_text: str
    scope_warning: str = ""
    fallback_reason: str = ""
    fallback_reason_code: str = ""
    max_rows: int | None = None


class ClarificationResolution(BaseModel):
    kind: Literal["column_resolution", "sheet_resolution"] = "column_resolution"
    source_field: str | None = None
    selected_value: str


class SpreadsheetChatRequest(BaseModel):
    file_id: str
    chat_text: str
    mode: Literal["auto", "text", "chart"] = "auto"
    sheet_index: int = 1
    sheet_override: bool = False
    locale: str = "en"
    conversation_id: str | None = None
    clarification_resolution: ClarificationResolution | None = None
    followup_action: Literal["continue_next_step"] | None = None
