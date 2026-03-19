from __future__ import annotations

import math
import re
from datetime import date, datetime, timedelta
from typing import Any

from ..core.numeric_coercion import coerce_float


EXCEL_SERIAL_MIN = 20000
EXCEL_SERIAL_MAX = 80000


def _parse_compact_year_month(text: str) -> datetime | None:
    compact = str(text or "").strip()
    if not re.fullmatch(r"\d{6}", compact):
        return None
    try:
        year = int(compact[:4])
        month = int(compact[4:6])
    except Exception:
        return None
    if year < 1900 or year > 2100 or month < 1 or month > 12:
        return None
    try:
        return datetime(year, month, 1)
    except Exception:
        return None


def _excel_serial_to_datetime(value: Any) -> datetime | None:
    try:
        serial = float(value)
    except Exception:
        return None
    if math.isnan(serial) or serial < EXCEL_SERIAL_MIN or serial > EXCEL_SERIAL_MAX:
        return None
    base = datetime(1899, 12, 30)
    days = int(math.floor(serial))
    seconds = int(round((serial - days) * 86400))
    return base + timedelta(days=days, seconds=seconds)


def coerce_datetime_value(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        try:
            numeric_value = float(value)
            if math.isnan(numeric_value):
                return None
            if numeric_value.is_integer():
                text = str(int(numeric_value))
                compact_month = _parse_compact_year_month(text)
                if compact_month is not None:
                    return compact_month
                if re.fullmatch(r"\d{14}", text):
                    return datetime.strptime(text, "%Y%m%d%H%M%S")
                if re.fullmatch(r"\d{12}", text):
                    return datetime.strptime(text, "%Y%m%d%H%M")
                if re.fullmatch(r"\d{8}", text):
                    return datetime.strptime(text, "%Y%m%d")
            serial_dt = _excel_serial_to_datetime(numeric_value)
            if serial_dt is not None:
                return serial_dt
        except Exception:
            return None
    text = str(value).strip()
    if not text:
        return None
    text = re.sub(r"\.0+$", "", text)
    compact_month = _parse_compact_year_month(text)
    if compact_month is not None:
        return compact_month
    compact = re.sub(r"\s+", "", text)
    for fmt, pattern in (("%Y%m%d%H%M%S", r"\d{14}"), ("%Y%m%d%H%M", r"\d{12}"), ("%Y%m%d", r"\d{8}")):
        if re.fullmatch(pattern, compact):
            try:
                return datetime.strptime(compact, fmt)
            except Exception:
                pass
    serial_dt = _excel_serial_to_datetime(coerce_float(text))
    if serial_dt is not None:
        return serial_dt
    normalized = text
    normalized = normalized.replace("年", "-").replace("月", "-").replace("日", "")
    normalized = normalized.replace("时", ":").replace("分", ":").replace("秒", "")
    normalized = normalized.replace(".", "-").replace("T", " ")
    normalized = re.sub(r"\s+", " ", normalized).strip()
    normalized = re.sub(r"-+", "-", normalized)
    normalized = re.sub(r":+", ":", normalized)
    normalized = normalized.rstrip(":")
    for fmt in (
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%Y-%m",
        "%Y/%m",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y/%m/%d %H:%M",
    ):
        try:
            return datetime.strptime(normalized, fmt)
        except Exception:
            continue
    try:
        return datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except Exception:
        return None


def compare_values(left: Any, right: Any) -> int:
    left_dt = coerce_datetime_value(left)
    right_dt = coerce_datetime_value(right)
    if left_dt is not None and right_dt is not None:
        if left_dt < right_dt:
            return -1
        if left_dt > right_dt:
            return 1
        return 0

    left_num = coerce_float(left)
    right_num = coerce_float(right)
    if left_num is not None and right_num is not None:
        if left_num < right_num:
            return -1
        if left_num > right_num:
            return 1
        return 0

    left_str = "" if left is None else str(left)
    right_str = "" if right is None else str(right)
    if left_str < right_str:
        return -1
    if left_str > right_str:
        return 1
    return 0


def safe_numeric_series(value: Any) -> Any:
    import pandas as pd

    if isinstance(value, (int, float)):
        return value
    base = pd.to_numeric(value, errors="coerce")
    if not hasattr(value, "map"):
        return base
    try:
        mapped = pd.to_numeric(value.map(coerce_float), errors="coerce")
        if mapped.notna().sum() > base.notna().sum():
            return mapped
    except Exception:
        return base
    return base


def coerce_datetime_series(series: Any) -> Any:
    import pandas as pd

    try:
        mapped = series.map(coerce_datetime_value)
    except Exception:
        mapped = series
    return pd.to_datetime(mapped, errors="coerce")


def pick_series_order(series: Any, *, semantic_type: str = "", literal: Any = None) -> tuple[str, Any]:
    numeric = safe_numeric_series(series)
    numeric_valid = int(numeric.notna().sum()) if hasattr(numeric, "notna") else 0
    dt = coerce_datetime_series(series)
    dt_valid = int(dt.notna().sum()) if hasattr(dt, "notna") else 0

    literal_dt = coerce_datetime_value(literal) if literal is not None else None
    literal_num = coerce_float(literal) if literal is not None else None

    if semantic_type == "date" and dt_valid:
        return "datetime", dt
    if semantic_type == "numeric" and numeric_valid:
        return "numeric", numeric
    if literal_dt is not None and dt_valid >= numeric_valid:
        return "datetime", dt
    if literal_num is not None and numeric_valid:
        return "numeric", numeric
    if dt_valid > numeric_valid:
        return "datetime", dt
    if numeric_valid:
        return "numeric", numeric
    return "text", series.astype(str)


def sort_frame_by_column(df: Any, column: str, direction: str, *, semantic_type: str = "") -> Any:
    sort_mode, sort_values = pick_series_order(df[column], semantic_type=semantic_type)
    if sort_mode == "text":
        return df.sort_values(by=column, ascending=(direction == "asc"), kind="mergesort")
    temp_column = "__sort_key__"
    out = df.assign(**{temp_column: sort_values})
    out = out.sort_values(by=temp_column, ascending=(direction == "asc"), kind="mergesort", na_position="last")
    return out.drop(columns=[temp_column])


def _is_blankish(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    if isinstance(value, str) and not value.strip():
        return True
    return False


def pick_extreme_value(series: Any, *, want_max: bool) -> Any:
    best = None
    for value in list(series):
        if _is_blankish(value):
            continue
        if best is None:
            best = value
            continue
        compare = compare_values(value, best)
        if want_max and compare > 0:
            best = value
        if (not want_max) and compare < 0:
            best = value
    if _is_blankish(best):
        return float("nan")
    numeric = coerce_float(best)
    if numeric is not None:
        return float(numeric)
    return best
