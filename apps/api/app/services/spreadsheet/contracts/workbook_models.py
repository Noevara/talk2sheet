from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class WorkbookSheetProfile(BaseModel):
    sheet_index: int
    sheet_name: str
    columns: list[str] = Field(default_factory=list)
    total_rows: int | None = None
    preview_row_count: int = 0
    column_profile_summary: list[dict[str, Any]] = Field(default_factory=list)


class WorkbookContext(BaseModel):
    file_id: str = ""
    active_sheet_index: int = 1
    active_sheet_name: str = ""
    sheets: list[WorkbookSheetProfile] = Field(default_factory=list)
