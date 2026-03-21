from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import pandas as pd

from app.schemas import SheetDescriptor
from .column_profile import attach_column_profiles
from .column_profile import get_column_profiles
from .loader_common import path_cache_key, read_csv_frame
from .row_count_cache import count_sheet_rows


def _field_summary(df: pd.DataFrame, *, limit: int = 6) -> list[str]:
    if df is None:
        return []
    enriched = attach_column_profiles(df)
    profiles = get_column_profiles(enriched)
    summary: list[str] = []
    for column in [str(item) for item in enriched.columns[: max(1, int(limit))]]:
        profile = profiles.get(column) or {}
        semantic_type = str(profile.get("semantic_type") or "").strip().lower()
        semantic_hints = [str(item).strip().lower() for item in (profile.get("semantic_hints") or []) if str(item).strip()]
        semantic_label = semantic_hints[0] if semantic_hints else semantic_type
        if semantic_label and semantic_label not in {"unknown", "text"}:
            summary.append(f"{column} ({semantic_label})")
        else:
            summary.append(column)
    return summary


def read_sheet_descriptors(path: Path) -> list[SheetDescriptor]:
    payloads = _read_sheet_descriptors_cached(path_cache_key(path))
    return [SheetDescriptor.model_validate(payload) for payload in payloads]


@lru_cache(maxsize=64)
def _read_sheet_descriptors_cached(cache_key: tuple[str, int, int]) -> tuple[dict[str, Any], ...]:
    path = Path(cache_key[0])
    if path.suffix.lower() == ".csv":
        df = read_csv_frame(path, header_row=0, nrows=30)
        return (
            {
                "index": 1,
                "name": path.stem,
                "rows": count_sheet_rows(path, sheet_index=1, header_plan=None),
                "columns": len(df.columns),
                "field_summary": _field_summary(df),
            },
        )

    sheets: list[dict[str, Any]] = []
    with pd.ExcelFile(path) as workbook:
        for idx, sheet_name in enumerate(workbook.sheet_names, start=1):
            preview = pd.read_excel(workbook, sheet_name=sheet_name, nrows=30)
            sheets.append(
                {
                    "index": idx,
                    "name": sheet_name,
                    "rows": count_sheet_rows(path, sheet_index=idx, header_plan=None),
                    "columns": len(preview.columns),
                    "field_summary": _field_summary(preview),
                }
            )
    return tuple(sheets)
