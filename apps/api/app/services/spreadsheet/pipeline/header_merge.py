from __future__ import annotations

import pandas as pd

from .header_detection import dedupe_columns, merge_header_rows


def coerce_obvious_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for column in out.columns:
        try:
            numeric = pd.to_numeric(out[column], errors="coerce")
        except Exception:
            continue
        if float(numeric.notna().mean()) >= 0.8:
            out[column] = numeric
    return out


def apply_header_rows(
    raw: pd.DataFrame,
    *,
    header_row_1based: int,
    header_depth: int,
    max_rows: int | None,
) -> pd.DataFrame:
    start = max(0, header_row_1based - 1)
    header_rows = raw.iloc[start : start + header_depth].fillna("").values.tolist()
    merged_columns = dedupe_columns(merge_header_rows(header_rows))
    data_start = start + header_depth
    data = raw.iloc[data_start:] if max_rows is None else raw.iloc[data_start : data_start + max_rows]
    data = data.copy().reset_index(drop=True)
    if len(merged_columns) < len(data.columns):
        merged_columns.extend(f"Column{index}" for index in range(len(merged_columns) + 1, len(data.columns) + 1))
    data.columns = merged_columns[: len(data.columns)]
    data = data.dropna(axis=0, how="all").reset_index(drop=True)
    return coerce_obvious_numeric_columns(data)
