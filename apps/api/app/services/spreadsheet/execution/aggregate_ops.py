from __future__ import annotations

from typing import Any

from ..core.schema import Metric, TransformPlan
from ..pipeline.column_profile import get_column_profiles
from .column_resolution import ensure_columns_exist, pick_close_column
from .value_coercion import pick_extreme_value, safe_numeric_series


def compute_metric(df: Any, metric: Metric) -> Any:
    agg = metric.agg
    profiles = get_column_profiles(df)
    if agg == "count_rows":
        return int(len(df))
    if agg in {"count_distinct", "nunique"}:
        if not metric.col:
            raise ValueError(f"{agg} requires col")
        column = pick_close_column(metric.col, list(df.columns), profiles=profiles)
        return int(df[column].nunique(dropna=True))
    if agg in {"sum", "avg", "min", "max"}:
        if not metric.col:
            raise ValueError(f"{agg} requires col")
        column = pick_close_column(metric.col, list(df.columns), profiles=profiles)
        series = df[column]
        numeric = safe_numeric_series(series)
        if agg == "sum":
            return float(numeric.fillna(0).sum())
        if agg == "avg":
            value = numeric.mean()
            try:
                return float(value)
            except Exception:
                return float("nan")
        if agg == "min":
            return pick_extreme_value(series, want_max=False)
        return pick_extreme_value(series, want_max=True)
    raise ValueError(f"Unsupported metric agg: {agg}")


def compute_grouped_metrics(working_df: Any, plan: TransformPlan, metrics: list[Metric]) -> tuple[Any, dict[str, Any]]:
    import pandas as pd

    meta: dict[str, Any] = {}
    if plan.groupby:
        group_columns = ensure_columns_exist(working_df, plan.groupby)
        try:
            working_df = working_df.dropna(subset=group_columns)
            meta["dropped_null_groupby_rows"] = True
        except Exception:
            meta["dropped_null_groupby_rows"] = False
        grouped = working_df.groupby(group_columns, dropna=False)
        rows: list[dict[str, Any]] = []
        for keys, chunk in grouped:
            if not isinstance(keys, tuple):
                keys = (keys,)
            row = {group_columns[index]: keys[index] for index in range(len(group_columns))}
            for metric in metrics:
                row[metric.as_name] = compute_metric(chunk, metric)
            rows.append(row)
        if rows:
            out = pd.DataFrame(rows)
        else:
            out = pd.DataFrame(columns=[*group_columns, *[metric.as_name for metric in metrics]])
        meta["groupby"] = group_columns
        return out, meta

    row = {}
    for metric in metrics:
        row[metric.as_name] = compute_metric(working_df, metric)
    return pd.DataFrame([row]), {"groupby": []}
