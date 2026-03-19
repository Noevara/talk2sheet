from __future__ import annotations

from pathlib import Path
from typing import Any

from .executor import apply_selection, apply_transform
from ..core.schema import SelectionPlan, TransformPlan
from .exact_metadata import (
    attach_exact_source_header_plan,
    build_exact_execution_meta,
    build_exact_execution_source_meta,
)
from .exact_source import exact_execution_source_context, load_exact_source_dataframe
from .exact_support import exact_execution_support


def execute_exact_plan(df: Any, selection_plan: SelectionPlan, transform_plan: TransformPlan) -> tuple[Any, dict[str, Any]]:
    selected_df, selection_meta = apply_selection(df, selection_plan)
    result_df, transform_meta = apply_transform(selected_df, transform_plan)
    return result_df, build_exact_execution_meta(selection_meta=selection_meta, transform_meta=transform_meta)


def execute_exact_plan_with_source_df(
    *,
    source_df: Any,
    source_sheet_name: str,
    source_path: Path | None,
    source_sheet_index: int,
    selection_plan: SelectionPlan,
    transform_plan: TransformPlan,
) -> tuple[Any, dict[str, Any]]:
    selected_df, selection_meta = apply_selection(source_df, selection_plan)
    result_df, transform_meta = apply_transform(selected_df, transform_plan)
    return result_df, build_exact_execution_source_meta(
        source_df=source_df,
        source_sheet_name=source_sheet_name,
        source_path=source_path,
        source_sheet_index=source_sheet_index,
        selection_meta=selection_meta,
        transform_meta=transform_meta,
    )


def execute_exact_plan_from_source(
    *,
    source_path: Path,
    source_sheet_index: int,
    selection_plan: SelectionPlan,
    transform_plan: TransformPlan,
    header_plan: dict[str, Any] | None = None,
) -> tuple[Any, dict[str, Any]]:
    source_df, sheet_name = load_exact_source_dataframe(
        source_path=source_path,
        source_sheet_index=source_sheet_index,
        header_plan=header_plan,
    )
    result_df, meta = execute_exact_plan_with_source_df(
        source_df=source_df,
        source_sheet_name=sheet_name,
        source_path=source_path,
        source_sheet_index=source_sheet_index,
        selection_plan=selection_plan,
        transform_plan=transform_plan,
    )
    return result_df, attach_exact_source_header_plan(meta, source_df=source_df, fallback_header_plan=header_plan)
