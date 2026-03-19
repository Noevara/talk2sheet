from __future__ import annotations

from typing import Any

from ..core.numeric_coercion import coerce_float
from ..core.schema import Filter
from ..pipeline.column_profile import get_column_profiles
from .column_resolution import pick_close_column
from .value_coercion import coerce_datetime_value, pick_series_order


def apply_filter(df: Any, flt: Filter) -> Any:
    import pandas as pd

    profiles = get_column_profiles(df)
    column = pick_close_column(flt.col, list(df.columns), profiles=profiles)
    semantic_type = str((profiles.get(column) or {}).get("semantic_type") or "")
    op = flt.op
    series = df[column]
    sort_mode, parsed = pick_series_order(series, semantic_type=semantic_type, literal=flt.value)
    literal_num = coerce_float(flt.value)
    literal_dt = coerce_datetime_value(flt.value)

    if op in {"=", "!="}:
        if sort_mode == "datetime" and literal_dt is not None:
            mask = parsed == pd.Timestamp(literal_dt)
        elif sort_mode == "numeric" and literal_num is not None:
            mask = parsed == literal_num
        else:
            mask = series.astype(str) == str(flt.value)
        return df[~mask] if op == "!=" else df[mask]

    if op in {">", ">=", "<", "<="}:
        if sort_mode == "datetime" and literal_dt is not None:
            value = pd.Timestamp(literal_dt)
            if op == ">":
                return df[parsed > value]
            if op == ">=":
                return df[parsed >= value]
            if op == "<":
                return df[parsed < value]
            return df[parsed <= value]
        if sort_mode == "numeric" and literal_num is not None:
            if op == ">":
                return df[parsed > literal_num]
            if op == ">=":
                return df[parsed >= literal_num]
            if op == "<":
                return df[parsed < literal_num]
            return df[parsed <= literal_num]
        if op == ">":
            return df[series > flt.value]
        if op == ">=":
            return df[series >= flt.value]
        if op == "<":
            return df[series < flt.value]
        return df[series <= flt.value]

    if op == "in":
        values = flt.value if isinstance(flt.value, list) else [flt.value]
        return df[series.isin(values)]
    if op == "contains":
        return df[series.astype(str).str.contains(str(flt.value), na=False)]
    raise ValueError(f"Unsupported filter op: {op}")
