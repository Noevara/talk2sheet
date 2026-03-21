from __future__ import annotations

from datetime import date, timedelta
import re
from typing import Any

import pandas as pd

from ..core.schema import Filter
from ..execution.executor import _coerce_datetime_series
from .planner_text_utils import _contains_any

_CN_NUMBERS = {
    "一": 1,
    "二": 2,
    "三": 3,
    "四": 4,
    "五": 5,
    "六": 6,
    "七": 7,
    "八": 8,
    "九": 9,
    "十": 10,
    "两": 2,
}


def _extract_month_literal(chat_text: str) -> str | None:
    text = str(chat_text or "")
    for pattern in (
        r"(\d{4}-\d{1,2})(?!-\d)",
        r"(\d{4}/\d{1,2})(?!/\d)",
        r"(\d{4}年\d{1,2}月)",
    ):
        match = re.search(pattern, text)
        if not match:
            continue
        raw = match.group(1)
        normalized = raw.replace("/", "-").replace("年", "-").replace("月", "")
        parts = normalized.split("-")
        if len(parts) == 2:
            year, month = parts
            return f"{int(year):04d}-{int(month):02d}"
    return None


def _extract_month_literals(chat_text: str) -> list[str]:
    text = str(chat_text or "")
    found: list[str] = []
    for pattern in (
        r"(\d{4}-\d{1,2})(?!-\d)",
        r"(\d{4}/\d{1,2})(?!/\d)",
        r"(\d{4}年\d{1,2}月)",
    ):
        for match in re.finditer(pattern, text):
            raw = match.group(1)
            normalized = raw.replace("/", "-").replace("年", "-").replace("月", "")
            parts = normalized.split("-")
            if len(parts) != 2:
                continue
            year, month = parts
            found.append(f"{int(year):04d}-{int(month):02d}")
    return list(dict.fromkeys(found))


_CN_MONTH_DIGITS = {
    "一": 1,
    "二": 2,
    "三": 3,
    "四": 4,
    "五": 5,
    "六": 6,
    "七": 7,
    "八": 8,
    "九": 9,
    "十": 10,
    "十一": 11,
    "十二": 12,
}


def _extract_month_numbers(chat_text: str) -> list[int]:
    text = str(chat_text or "")
    found: list[int] = []
    for match in re.finditer(r"(?<!\d)(1[0-2]|0?[1-9])\s*月(?:份)?", text):
        found.append(int(match.group(1)))
    for token, month in _CN_MONTH_DIGITS.items():
        if re.search(rf"{re.escape(token)}月(?:份)?", text):
            found.append(month)
    for token, month in (
        ("january", 1),
        ("february", 2),
        ("march", 3),
        ("april", 4),
        ("may", 5),
        ("june", 6),
        ("july", 7),
        ("august", 8),
        ("september", 9),
        ("october", 10),
        ("november", 11),
        ("december", 12),
        ("jan", 1),
        ("feb", 2),
        ("mar", 3),
        ("apr", 4),
        ("jun", 6),
        ("jul", 7),
        ("aug", 8),
        ("sep", 9),
        ("oct", 10),
        ("nov", 11),
        ("dec", 12),
    ):
        if re.search(rf"\b{token}\b", text, flags=re.I):
            found.append(month)
    return list(dict.fromkeys(month for month in found if 1 <= month <= 12))


def _parse_chinese_small_number(token: str) -> int | None:
    text = str(token or "").strip()
    if not text:
        return None
    if text.isdigit():
        value = int(text)
        return value if value > 0 else None
    if text in _CN_NUMBERS:
        return _CN_NUMBERS[text]
    if text.startswith("十"):
        tail = text[1:]
        if not tail:
            return 10
        if tail in _CN_NUMBERS:
            return 10 + _CN_NUMBERS[tail]
    if text.endswith("十") and text[0] in _CN_NUMBERS:
        return _CN_NUMBERS[text[0]] * 10
    if len(text) == 2 and text[0] in _CN_NUMBERS and text[1] in _CN_NUMBERS:
        return _CN_NUMBERS[text[0]] * 10 + _CN_NUMBERS[text[1]]
    return None


def _available_month_buckets(df: Any, *, date_column: str) -> list[str]:
    if date_column not in getattr(df, "columns", []):
        return []
    try:
        series = _coerce_datetime_series(df[date_column])
    except Exception:
        return []
    buckets = [value for value in series.dt.strftime("%Y-%m").dropna().tolist() if str(value or "").strip()]
    return list(dict.fromkeys(buckets))


def _time_bucket_alias(grain: str) -> str:
    normalized = str(grain or "").strip().lower()
    if not normalized:
        return "time_bucket"
    if normalized.endswith("_bucket"):
        return normalized
    return f"{normalized}_bucket"


def _resolve_requested_month_buckets(df: Any, *, date_column: str | None, chat_text: str) -> list[str]:
    if not date_column:
        return []
    explicit = _extract_month_literals(chat_text)
    if explicit:
        return explicit
    month_numbers = _extract_month_numbers(chat_text)
    if len(month_numbers) < 2:
        return []
    available = _available_month_buckets(df, date_column=date_column)
    if not available:
        return []
    unique_years = sorted({item.split("-")[0] for item in available if "-" in item})
    if len(unique_years) == 1:
        year = unique_years[0]
        resolved = [f"{year}-{month:02d}" for month in month_numbers]
        return [value for value in resolved if value in available]
    resolved = [value for value in available if any(value.endswith(f"-{month:02d}") for month in month_numbers)]
    return list(dict.fromkeys(resolved))


def _resolve_requested_single_month_bucket(df: Any, *, date_column: str | None, chat_text: str) -> str | None:
    if not date_column:
        return None
    available = sorted(_available_month_buckets(df, date_column=date_column))
    if not available:
        return None

    explicit_literals = _extract_month_literals(chat_text)
    if len(explicit_literals) > 1:
        return None
    if explicit_literals:
        return explicit_literals[0]
    month_numbers = _extract_month_numbers(chat_text)
    if len(month_numbers) > 1:
        return None
    if not month_numbers:
        text = str(chat_text or "")
        if _contains_any(text, ("上个月", "上月", "last month", "previous month")):
            return available[-2] if len(available) >= 2 else None
        if _contains_any(text, ("这个月", "本月", "current month", "this month")):
            return available[-1]
        return None
    unique_years = sorted({item.split("-")[0] for item in available if "-" in item})
    month = month_numbers[0]
    if len(unique_years) == 1:
        resolved = f"{unique_years[0]}-{month:02d}"
        return resolved if resolved in available else None
    matches = [value for value in available if value.endswith(f"-{month:02d}")]
    if not matches:
        return None
    return sorted(matches)[-1]


def _build_month_range_filters(date_column: str, month_bucket: str) -> list[Filter]:
    text = str(month_bucket or "").strip()
    if not text:
        return [Filter(col=date_column, op="contains", value=month_bucket)]
    try:
        period = pd.Period(text, freq="M")
    except Exception:
        return [Filter(col=date_column, op="contains", value=month_bucket)]
    start = str(period.start_time.date())
    end = str((period + 1).start_time.date())
    return [
        Filter(col=date_column, op=">=", value=start),
        Filter(col=date_column, op="<", value=end),
    ]


def _extract_date_literal(chat_text: str) -> str | None:
    text = str(chat_text or "")
    for pattern in (
        r"(\d{4}-\d{1,2}-\d{1,2})",
        r"(\d{4}/\d{1,2}/\d{1,2})",
        r"(\d{4}年\d{1,2}月\d{1,2}日)",
    ):
        match = re.search(pattern, text)
        if not match:
            continue
        raw = match.group(1)
        normalized = raw.replace("/", "-").replace("年", "-").replace("月", "-").replace("日", "")
        parts = normalized.split("-")
        if len(parts) == 3:
            year, month, day = parts
            return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
    return None


def _extract_week_literal(chat_text: str) -> str | None:
    text = str(chat_text or "")
    matched = re.search(r"(\d{4})[-/年]?[Ww第]?\s*(\d{1,2})\s*(?:周|week|w)?", text)
    if not matched:
        return None
    year = int(matched.group(1))
    week = int(matched.group(2))
    if not (1 <= week <= 53):
        return None
    return f"{year:04d}-W{week:02d}"


def _previous_period_literal(value: str, *, grain: str, basis: str) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    normalized_basis = str(basis or "").strip().lower()

    if grain == "day":
        matched = _extract_date_literal(text)
        if not matched:
            return None
        year, month, day = [int(item) for item in matched.split("-")]
        try:
            current = date(year, month, day)
            previous = date(year - 1, month, day) if normalized_basis == "year_over_year" else current - timedelta(days=1)
            return previous.isoformat()
        except Exception:
            return None

    if grain == "week":
        matched = _extract_week_literal(text)
        if not matched:
            return None
        week_match = re.search(r"(\d{4})-W(\d{2})", matched)
        if not week_match:
            return None
        year = int(week_match.group(1))
        week = int(week_match.group(2))
        if normalized_basis == "year_over_year":
            return f"{year - 1:04d}-W{week:02d}"
        week -= 1
        if week >= 1:
            return f"{year:04d}-W{week:02d}"
        return f"{year - 1:04d}-W52"

    if grain == "month":
        matched = _extract_month_literal(text)
        if not matched:
            return None
        year, month = [int(item) for item in matched.split("-")]
        if normalized_basis == "year_over_year":
            return f"{year - 1:04d}-{month:02d}"
        month -= 1
        if month >= 1:
            return f"{year:04d}-{month:02d}"
        return f"{year - 1:04d}-12"

    return None


def _extract_time_grain(chat_text: str, default: str = "month") -> str:
    text = str(chat_text or "").lower()
    if _extract_recent_period_count(text, grain="day"):
        return "day"
    if _extract_recent_period_count(text, grain="week"):
        return "week"
    if _extract_recent_period_count(text, grain="month"):
        return "month"
    if _contains_any(text, ("按天", "每天", "日报", "日度", "daily", "per day", "day trend", "daily trend")):
        return "day"
    if _contains_any(text, ("按周", "每周", "周度", "weekly", "per week", "week trend", "weekly trend", "按星期", "每星期")):
        return "week"
    if _contains_any(text, ("按季度", "每季度", "quarterly", "per quarter")):
        return "quarter"
    if _contains_any(text, ("工作日", "周末", "weekday", "weekend", "weekpart")):
        return "weekpart"
    if _contains_any(text, ("星期", "周几", "weekday")):
        return "weekday"
    if _contains_any(text, ("按月", "每月", "月度", "monthly", "per month", "month trend", "monthly trend", "月份", "趋势")):
        return "month"
    return default


def _extract_recent_period_count(chat_text: str, *, grain: str | None = None) -> int | None:
    text = str(chat_text or "")
    lowered = text.lower()

    for matched in re.finditer(r"(?:最近|近)\s*(\d{1,2}|[一二三四五六七八九十两]{1,3})\s*(天|日|周|星期|个?月)", text):
        raw_count = matched.group(1)
        unit = matched.group(2)
        count = _parse_chinese_small_number(raw_count)
        if count is None:
            continue
        unit_grain = "month" if "月" in unit else "week" if unit in {"周", "星期"} else "day"
        if grain and unit_grain != grain:
            continue
        return count

    for matched in re.finditer(r"\b(?:last|past|recent)\s+(\d{1,2})\s*(days?|weeks?|months?)\b", lowered):
        count = int(matched.group(1))
        unit = matched.group(2)
        unit_grain = "month" if unit.startswith("month") else "week" if unit.startswith("week") else "day"
        if grain and unit_grain != grain:
            continue
        return count

    return None


def _available_time_buckets(df: Any, *, date_column: str, grain: str) -> list[str]:
    if date_column not in getattr(df, "columns", []):
        return []
    try:
        series = _coerce_datetime_series(df[date_column])
    except Exception:
        return []
    valid = series.dropna()
    if valid.empty:
        return []
    if grain == "day":
        buckets = valid.dt.strftime("%Y-%m-%d").tolist()
    elif grain == "week":
        iso = valid.dt.isocalendar()
        buckets = (iso.year.astype("Int64").astype(str) + "-W" + iso.week.astype("Int64").astype(str).str.zfill(2)).tolist()
    elif grain == "month":
        buckets = valid.dt.strftime("%Y-%m").tolist()
    else:
        return []
    return list(dict.fromkeys(str(value) for value in buckets if str(value or "").strip()))


def _resolve_recent_period_buckets(df: Any, *, date_column: str, grain: str, count: int) -> list[str]:
    if count <= 0:
        return []
    available = sorted(_available_time_buckets(df, date_column=date_column, grain=grain))
    if not available:
        return []
    return available[-count:]


def _next_period_literal(current: str, *, grain: str, steps: int = 1) -> str | None:
    text = str(current or "").strip()
    if not text:
        return None
    if grain == "day":
        base = pd.Period(text, freq="D")
        return str(base + steps)
    if grain == "month":
        base = pd.Period(text, freq="M")
        return str(base + steps)
    if grain == "week":
        matched = re.match(r"^(\d{4})-W(\d{2})$", text)
        if not matched:
            return None
        current_date = date.fromisocalendar(int(matched.group(1)), int(matched.group(2)), 1)
        next_date = current_date + timedelta(weeks=steps)
        iso = next_date.isocalendar()
        return f"{iso.year:04d}-W{iso.week:02d}"
    return None


def _resolve_forecast_targets(df: Any, *, date_column: str | None, chat_text: str, grain: str) -> tuple[list[str], int] | None:
    if not date_column or grain not in {"day", "week", "month"}:
        return None
    available = _available_time_buckets(df, date_column=date_column, grain=grain)
    if len(available) < 3:
        return None
    latest_period = sorted(available)[-1]
    target_period: str | None = None
    target_periods: list[str] | None = None
    if grain == "day":
        explicit_day = _extract_date_literal(chat_text)
        explicit_month = _extract_month_literal(chat_text)
        month_numbers = _extract_month_numbers(chat_text)
        if explicit_day:
            target_period = explicit_day
        elif _contains_any(chat_text, ("每天", "每日", "逐日", "每一天", "each day", "per day", "daily breakdown", "daily forecast")):
            target_month: str | None = None
            if explicit_month:
                target_month = explicit_month
            elif len(month_numbers) == 1:
                latest_year = latest_period.split("-")[0]
                target_month = f"{latest_year}-{month_numbers[0]:02d}"
            elif _contains_any(chat_text, ("剩余", "剩下", "余下", "后续", "remaining", "rest of", "the rest of")):
                target_month = latest_period[:7]

            if target_month:
                try:
                    month_period = pd.Period(target_month, freq="M")
                    month_start = month_period.start_time.to_period("D")
                    month_end = month_period.end_time.to_period("D")
                    latest_day = pd.Period(latest_period, freq="D")
                    if _contains_any(chat_text, ("剩余", "剩下", "余下", "后续", "remaining", "rest of", "the rest of")):
                        start_day = max(month_start, latest_day + 1)
                    else:
                        start_day = max(month_start, latest_day + 1) if month_start <= latest_day else month_start
                    if start_day <= month_end:
                        target_periods = [str(item) for item in pd.period_range(start=start_day, end=month_end, freq="D")]
                except Exception:
                    target_periods = None
        elif _contains_any(chat_text, ("明天", "next day", "下一天")):
            target_period = _next_period_literal(latest_period, grain="day")
    elif grain == "month":
        explicit_month = _extract_month_literal(chat_text)
        if explicit_month:
            target_period = explicit_month
        else:
            month_numbers = _extract_month_numbers(chat_text)
            if len(month_numbers) == 1:
                latest_year = latest_period.split("-")[0]
                target_period = f"{latest_year}-{month_numbers[0]:02d}"
            elif _contains_any(chat_text, ("下个月", "下月", "next month")):
                target_period = _next_period_literal(latest_period, grain="month")
    elif grain == "week":
        if _contains_any(chat_text, ("下周", "next week")):
            target_period = _next_period_literal(latest_period, grain="week")
    if target_periods:
        filtered = [item for item in target_periods if item > latest_period]
        if not filtered:
            return None
        return filtered, max(1, len(filtered))
    if not target_period:
        target_period = _next_period_literal(latest_period, grain=grain)
    if not target_period or target_period <= latest_period:
        return None
    if grain == "day":
        horizon = (pd.Period(target_period, freq="D") - pd.Period(latest_period, freq="D")).n
    elif grain == "month":
        horizon = (pd.Period(target_period, freq="M") - pd.Period(latest_period, freq="M")).n
    else:
        matched_target = re.match(r"^(\d{4})-W(\d{2})$", target_period)
        matched_latest = re.match(r"^(\d{4})-W(\d{2})$", latest_period)
        if not matched_target or not matched_latest:
            return None
        target_date = date.fromisocalendar(int(matched_target.group(1)), int(matched_target.group(2)), 1)
        latest_date = date.fromisocalendar(int(matched_latest.group(1)), int(matched_latest.group(2)), 1)
        horizon = max(1, int((target_date - latest_date).days // 7))
    return [target_period], max(1, int(horizon))
