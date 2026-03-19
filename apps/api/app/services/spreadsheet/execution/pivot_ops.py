from __future__ import annotations

from typing import Any

from ..core.schema import Filter, PivotSpec
from ..pipeline.column_profile import attach_column_profiles, get_column_profiles
from .column_resolution import pick_close_column, resolve_column_reference
from .filter_ops import apply_filter


def apply_having(df: Any, having: list[Filter]) -> tuple[Any, list[dict[str, Any]]]:
    if not having:
        return df, []
    out = df
    applied: list[dict[str, Any]] = []
    for flt in having:
        profiles = get_column_profiles(out)
        resolved = resolve_column_reference(flt.col, list(out.columns), profiles=profiles)
        resolved_column = str(resolved.get("resolved") or "")
        if not resolved_column or resolved.get("confidence") == "low":
            raise ValueError(f"Column resolution too weak for having: requested={flt.col} resolved={resolved_column}")
        out = apply_filter(out, flt)
        applied.append({"col": flt.col, "resolved_col": resolved_column, "op": flt.op, "value": flt.value})
    return attach_column_profiles(out), applied


def _flatten_columns(columns: Any) -> list[str]:
    flattened: list[str] = []
    for column in list(columns):
        if isinstance(column, tuple):
            parts = [str(item) for item in column if str(item or "").strip()]
            flattened.append("/".join(parts))
        else:
            flattened.append(str(column))
    return flattened


def apply_pivot(df: Any, pivot: PivotSpec | None) -> tuple[Any, dict[str, Any] | None]:
    import pandas as pd

    if not pivot:
        return df, None

    out = df.copy()
    profiles = get_column_profiles(out)
    index_columns = [pick_close_column(column, list(out.columns), profiles=profiles) for column in (pivot.index or [])]
    pivot_column = pick_close_column(pivot.columns, list(out.columns), profiles=profiles)
    values_column = pick_close_column(pivot.values, list(out.columns), profiles=profiles)
    dummy_column = None
    if not index_columns:
        dummy_column = "__pivot_index__"
        out[dummy_column] = "All"
        index_columns = [dummy_column]
    out = pd.pivot_table(
        out,
        index=index_columns,
        columns=pivot_column,
        values=values_column,
        aggfunc="first",
        fill_value=pivot.fill_value,
    ).reset_index()
    out.columns = _flatten_columns(out.columns)
    if dummy_column and dummy_column in out.columns:
        out = out.drop(columns=[dummy_column])
    return attach_column_profiles(out), {
        "index": index_columns if not dummy_column else [],
        "columns": pivot_column,
        "values": values_column,
        "fill_value": pivot.fill_value,
    }
