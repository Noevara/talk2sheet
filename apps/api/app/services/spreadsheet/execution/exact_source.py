from __future__ import annotations

from pathlib import Path
from typing import Any

from ..pipeline import HEADER_PLAN_ATTR, SOURCE_PATH_ATTR, SOURCE_SHEET_INDEX_ATTR, load_full_dataframe


def load_exact_source_dataframe(
    *,
    source_path: Path,
    source_sheet_index: int,
    header_plan: dict[str, Any] | None = None,
) -> tuple[Any, str]:
    return load_full_dataframe(
        source_path,
        sheet_index=source_sheet_index,
        header_plan=header_plan,
    )


def exact_execution_source_context(df: Any) -> dict[str, Any]:
    source_path = df.attrs.get(SOURCE_PATH_ATTR)
    if not source_path:
        return {"available": False}
    return {
        "available": True,
        "source_path": str(source_path),
        "source_sheet_index": int(df.attrs.get(SOURCE_SHEET_INDEX_ATTR) or 1),
        "header_plan": df.attrs.get(HEADER_PLAN_ATTR) or {},
    }
