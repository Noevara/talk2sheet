from __future__ import annotations

from typing import Any

from ..pipeline.column_profile import attach_column_profiles, get_column_profiles
from ..core.schema import Metric, SelectionPlan, TransformPlan
from .column_resolution import (
    detect_unique_key_candidates,
    pick_close_column,
    resolve_column_reference,
)
from .derived_ops import apply_derived_columns
from .filter_ops import apply_filter
from .aggregate_ops import compute_grouped_metrics
from .transform_ops import apply_post_agg_operations
from .value_coercion import coerce_datetime_series as _coerce_datetime_series
from .value_coercion import sort_frame_by_column


def apply_selection(df: Any, plan: SelectionPlan) -> tuple[Any, dict[str, Any]]:
    profiles = get_column_profiles(df)
    meta: dict[str, Any] = {"applied": True, "rows_before": int(df.shape[0]), "cols_before": int(df.shape[1])}
    column_mapping: list[dict[str, Any]] = []
    out = df

    applied_filters = []
    for flt in plan.filters:
        resolved = resolve_column_reference(flt.col, list(out.columns), profiles=profiles)
        resolved_column = str(resolved.get("resolved") or "")
        column_mapping.append({**resolved, "where": "filter"})
        if not resolved_column or resolved.get("confidence") == "low":
            raise ValueError(f"Column resolution too weak for filter: requested={flt.col} resolved={resolved_column}")
        out = apply_filter(out, flt)
        applied_filters.append({"col": flt.col, "resolved_col": resolved_column, "op": flt.op, "value": flt.value})
        profiles = get_column_profiles(out)
    meta["filters"] = applied_filters

    if plan.distinct_by:
        resolved = resolve_column_reference(plan.distinct_by, list(out.columns), profiles=profiles)
        column = str(resolved.get("resolved") or "")
        column_mapping.append({**resolved, "where": "distinct_by"})
        if not column or resolved.get("confidence") == "low":
            raise ValueError(f"Column resolution too weak for distinct_by: requested={plan.distinct_by} resolved={column}")
        before = len(out)
        out = out.drop_duplicates(subset=[column])
        meta["distinct_by"] = column
        meta["distinct_dropped"] = before - len(out)
        profiles = get_column_profiles(out)

    if plan.sort:
        resolved = resolve_column_reference(plan.sort.col, list(out.columns), profiles=profiles)
        column = str(resolved.get("resolved") or "")
        column_mapping.append({**resolved, "where": "sort"})
        if not column or resolved.get("confidence") == "low":
            raise ValueError(f"Column resolution too weak for sort: requested={plan.sort.col} resolved={column}")
        semantic_type = str((profiles.get(column) or {}).get("semantic_type") or "")
        out = sort_frame_by_column(out, column, plan.sort.dir, semantic_type=semantic_type)
        meta["sort"] = {"col": column, "dir": plan.sort.dir}
        profiles = get_column_profiles(out)

    if plan.limit is not None:
        out = out.head(int(plan.limit))
        meta["limit"] = int(plan.limit)
        profiles = get_column_profiles(out)

    if plan.columns:
        columns: list[str] = []
        for column in plan.columns:
            resolved = resolve_column_reference(column, list(out.columns), profiles=profiles)
            resolved_column = str(resolved.get("resolved") or "")
            column_mapping.append({**resolved, "where": "project"})
            if not resolved_column or resolved.get("confidence") == "low":
                raise ValueError(f"Column resolution too weak for projection: requested={column} resolved={resolved_column}")
            columns.append(resolved_column)
        out = out[columns]
        meta["columns"] = columns
    else:
        meta["columns"] = [str(column) for column in out.columns]

    meta["column_mapping"] = column_mapping
    meta["rows"] = int(out.shape[0])
    meta["cols"] = int(out.shape[1])
    return attach_column_profiles(out), meta


def apply_transform(df: Any, plan: TransformPlan) -> tuple[Any, dict[str, Any]]:
    import pandas as pd

    meta: dict[str, Any] = {"applied": True}
    working_df = attach_column_profiles(df)

    if plan.derived_columns:
        working_df, derived_meta = apply_derived_columns(working_df, plan.derived_columns)
        meta["derived_columns"] = derived_meta

    if plan.return_rows:
        out = working_df.copy()
        meta["detail_mode"] = True
        meta["return_rows"] = True
        meta["groupby"] = []
        meta["metrics"] = []
        out, post_meta = apply_post_agg_operations(out, plan)
        meta.update(post_meta)
        meta["rows"] = int(out.shape[0])
        meta["cols"] = int(out.shape[1])
        return attach_column_profiles(out), meta

    metrics = plan.metrics or [Metric(agg="count_rows", col=None, as_name="count")]
    out, grouping_meta = compute_grouped_metrics(working_df, plan, metrics)
    meta.update(grouping_meta)

    if not isinstance(out, pd.DataFrame):
        out = pd.DataFrame(out)

    out, post_meta = apply_post_agg_operations(out, plan)
    meta.update(post_meta)
    meta["rows"] = int(out.shape[0])
    meta["cols"] = int(out.shape[1])
    meta["metrics"] = [{"agg": metric.agg, "col": metric.col, "as": metric.as_name} for metric in metrics]
    return attach_column_profiles(out), meta


__all__ = [
    "_coerce_datetime_series",
    "apply_selection",
    "apply_transform",
    "detect_unique_key_candidates",
    "pick_close_column",
    "resolve_column_reference",
]
