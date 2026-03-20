from __future__ import annotations

from pathlib import Path

from app.config import get_settings
from app.schemas import PreviewResponse
from ..core.serialization import dataframe_to_rows
from .dataframe_loader import load_dataframe
from .loader_common import (
    HEADER_PLAN_ATTR,
)
from .row_count_cache import count_sheet_rows


def preview_sheet(path: Path, *, file_id: str, sheet_index: int = 1) -> PreviewResponse:
    settings = get_settings()
    df, sheet_name = load_dataframe(path, sheet_index=sheet_index, limit=settings.max_preview_rows)
    rows = dataframe_to_rows(df.head(settings.max_preview_rows))
    columns = [str(column) for column in df.columns.tolist()]
    header_plan = df.attrs.get(HEADER_PLAN_ATTR) if isinstance(df.attrs.get(HEADER_PLAN_ATTR), dict) else None
    return PreviewResponse(
        file_id=file_id,
        sheet_index=max(1, int(sheet_index or 1)),
        sheet_name=sheet_name,
        columns=columns,
        rows=rows,
        total_rows=count_sheet_rows(path, sheet_index=sheet_index, header_plan=header_plan),
        preview_row_count=int(len(rows)),
    )
