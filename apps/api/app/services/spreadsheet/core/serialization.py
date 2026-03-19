from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal
from typing import Any

import pandas as pd


def serialize_cell(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, (pd.Timestamp, datetime, date, time)):
        return value.isoformat()
    if isinstance(value, pd.Timedelta):
        return str(value)
    if isinstance(value, pd.Period):
        return str(value)
    if isinstance(value, Decimal):
        return float(value)
    if pd.isna(value):
        return ""
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return str(value)
    return value


def dataframe_to_rows(df: pd.DataFrame) -> list[list[Any]]:
    return [[serialize_cell(item) for item in row] for row in df.itertuples(index=False, name=None)]


def dataframe_to_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    columns = [str(column) for column in df.columns]
    records: list[dict[str, Any]] = []
    for row in df.itertuples(index=False, name=None):
        records.append({columns[index]: serialize_cell(value) for index, value in enumerate(row)})
    return records
