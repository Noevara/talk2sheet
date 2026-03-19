from __future__ import annotations

from pathlib import Path
from typing import Any

from ..pipeline import HEADER_PLAN_ATTR


def build_exact_execution_meta(*, selection_meta: dict[str, Any], transform_meta: dict[str, Any]) -> dict[str, Any]:
    return {
        "eligible": True,
        "data_scope": "exact_full_table",
        "selection_meta": selection_meta,
        "transform_meta": transform_meta,
    }


def build_exact_execution_source_meta(
    *,
    source_df: Any,
    source_sheet_name: str,
    source_path: Path | None,
    source_sheet_index: int,
    selection_meta: dict[str, Any],
    transform_meta: dict[str, Any],
) -> dict[str, Any]:
    return {
        "eligible": True,
        "data_scope": "exact_full_table",
        "source_kind": "uploaded_file",
        "source_path": str(source_path) if source_path is not None else "",
        "source_sheet_index": int(source_sheet_index or 1),
        "source_sheet_name": source_sheet_name,
        "source_row_count": int(len(source_df.index)),
        "header_plan": source_df.attrs.get(HEADER_PLAN_ATTR) or {},
        "selection_meta": selection_meta,
        "transform_meta": transform_meta,
    }


def attach_exact_source_header_plan(
    meta: dict[str, Any],
    *,
    source_df: Any,
    fallback_header_plan: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        **meta,
        "header_plan": source_df.attrs.get(HEADER_PLAN_ATTR) or fallback_header_plan or {},
    }
