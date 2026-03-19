from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from app.config import get_settings
from .dataframe_finalize import finalize_loaded_dataframe
from .header_detection import maybe_detect_header_plan
from .header_merge import apply_header_rows
from .raw_file_reader import read_default_frame, read_raw_frame


def _normalize_header_window(*, header_row_1based: int, header_depth: int, max_rows: int | None) -> tuple[int, int, int | None]:
    row_1based = max(1, int(header_row_1based or 1))
    depth = max(1, int(header_depth or 1))
    nrows = None if max_rows is None else row_1based - 1 + depth + max_rows
    return row_1based, depth, nrows


def _load_header_aware_dataframe(
    path: Path,
    *,
    sheet_index: int,
    max_rows: int | None,
    header_row_1based: int,
    header_depth: int,
) -> tuple[pd.DataFrame, str]:
    row_1based, depth, nrows = _normalize_header_window(
        header_row_1based=header_row_1based,
        header_depth=header_depth,
        max_rows=max_rows,
    )
    raw, sheet_name = read_raw_frame(path, sheet_index=sheet_index, nrows=nrows)
    return apply_header_rows(
        raw,
        header_row_1based=row_1based,
        header_depth=depth,
        max_rows=max_rows,
    ), sheet_name


def _materialize_default_header_plan(plan: dict[str, Any]) -> dict[str, Any]:
    return {
        **plan,
        "has_header": True,
        "header_row_1based": 1,
        "header_depth": 1,
        "data_start_row_1based": 2,
    }


def load_dataframe(path: Path, sheet_index: int = 1, limit: int | None = None) -> tuple[pd.DataFrame, str]:
    settings = get_settings()
    row_limit = int(limit or settings.max_analysis_rows)
    header_plan = maybe_detect_header_plan(path, sheet_index=sheet_index)

    if header_plan.has_header and header_plan.header_row_1based is not None:
        df, sheet_name = _load_header_aware_dataframe(
            path,
            sheet_index=sheet_index,
            max_rows=row_limit,
            header_row_1based=header_plan.header_row_1based,
            header_depth=header_plan.header_depth,
        )
    else:
        df, sheet_name = read_default_frame(path, sheet_index=sheet_index, nrows=row_limit)
        header_plan = header_plan.model_copy(update=_materialize_default_header_plan({}))

    return finalize_loaded_dataframe(
        df,
        sheet_name=sheet_name,
        header_plan=header_plan.model_dump(),
        source_path=path,
        source_sheet_index=sheet_index,
    ), sheet_name


def load_full_dataframe(
    path: Path,
    *,
    sheet_index: int = 1,
    header_plan: dict[str, Any] | None = None,
) -> tuple[pd.DataFrame, str]:
    plan = header_plan or maybe_detect_header_plan(path, sheet_index=sheet_index).model_dump()
    if plan.get("has_header") and plan.get("header_row_1based") is not None:
        df, sheet_name = _load_header_aware_dataframe(
            path,
            sheet_index=sheet_index,
            max_rows=None,
            header_row_1based=int(plan["header_row_1based"]),
            header_depth=int(plan.get("header_depth") or 1),
        )
    else:
        df, sheet_name = read_default_frame(path, sheet_index=sheet_index, nrows=None)
        plan = _materialize_default_header_plan(plan)
    return finalize_loaded_dataframe(
        df,
        sheet_name=sheet_name,
        header_plan=plan,
        source_path=path,
        source_sheet_index=sheet_index,
    ), sheet_name
