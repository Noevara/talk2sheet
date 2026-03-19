from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

from ..contracts.workbook_models import WorkbookContext, WorkbookSheetProfile
from .column_profile import attach_column_profiles
from .column_profile import get_column_profiles
from .loader_common import path_cache_key
from .raw_file_reader import read_default_frame
from .row_count_cache import count_sheet_rows
from .sheet_metadata import read_sheet_descriptors


def _column_profile_summary(df: Any) -> list[dict[str, Any]]:
    profiles = get_column_profiles(df)
    summary: list[dict[str, Any]] = []
    for column in [str(item) for item in getattr(df, "columns", [])]:
        profile = profiles.get(column) or {}
        summary.append(
            {
                "name": column,
                "semantic_type": str(profile.get("semantic_type") or ""),
                "semantic_hints": [str(item) for item in (profile.get("semantic_hints") or [])],
            }
        )
    return summary


@lru_cache(maxsize=32)
def _read_workbook_context_cached(
    cache_key: tuple[str, int, int],
    *,
    preview_limit: int,
) -> tuple[dict[str, Any], ...]:
    path = Path(cache_key[0])
    sheet_payloads: list[dict[str, Any]] = []
    for descriptor in read_sheet_descriptors(path):
        df, sheet_name = read_default_frame(path, sheet_index=descriptor.index, nrows=preview_limit)
        df = attach_column_profiles(df)
        total_rows = count_sheet_rows(path, sheet_index=descriptor.index, header_plan=None)
        sheet_payloads.append(
            WorkbookSheetProfile(
                sheet_index=int(descriptor.index),
                sheet_name=sheet_name,
                columns=[str(column) for column in df.columns.tolist()],
                total_rows=total_rows,
                preview_row_count=int(len(df.index)),
                column_profile_summary=_column_profile_summary(df),
            ).model_dump()
        )
    return tuple(sheet_payloads)


def read_workbook_context(
    path: Path,
    *,
    file_id: str = "",
    active_sheet_index: int = 1,
    preview_limit: int = 8,
) -> WorkbookContext:
    sheet_payloads = _read_workbook_context_cached(
        path_cache_key(path),
        preview_limit=max(1, int(preview_limit)),
    )
    sheets = [WorkbookSheetProfile.model_validate(payload) for payload in sheet_payloads]
    resolved_active_index = max(1, int(active_sheet_index or 1))
    active_sheet = next((sheet for sheet in sheets if sheet.sheet_index == resolved_active_index), None)
    if active_sheet is None and sheets:
        active_sheet = sheets[0]
        resolved_active_index = active_sheet.sheet_index
    return WorkbookContext(
        file_id=file_id,
        active_sheet_index=resolved_active_index,
        active_sheet_name=active_sheet.sheet_name if active_sheet is not None else "",
        sheets=sheets,
    )
