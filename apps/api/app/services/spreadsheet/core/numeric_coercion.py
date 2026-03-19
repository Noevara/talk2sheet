from __future__ import annotations

import re
from typing import Any


def normalize_numeric_text(value: Any) -> tuple[str, float, bool] | None:
    raw = str(value or "").strip()
    if not raw:
        return None

    negative = False
    if raw.startswith("(") and raw.endswith(")"):
        negative = True
        raw = raw[1:-1].strip()

    raw = (
        raw.replace("人民币", "")
        .replace("RMB", "")
        .replace("CNY", "")
        .replace("￥", "")
        .replace("¥", "")
        .replace("$", "")
        .replace(",", "")
        .replace("，", "")
    )

    multiplier = 1.0
    if raw.endswith("%"):
        multiplier = 0.01
        raw = raw[:-1]

    for unit, factor in (("亿元", 1e8), ("万", 1e4), ("千", 1e3)):
        if raw.endswith(unit):
            multiplier *= factor
            raw = raw[: -len(unit)]
            break

    raw = re.sub(r"\s+", "", raw)
    match = re.fullmatch(r"[+-]?\d+(?:\.\d+)?", raw)
    if not match:
        return None
    return raw, multiplier, negative


def coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)

    normalized = normalize_numeric_text(value)
    if normalized is None:
        return None

    number_text, multiplier, negative = normalized
    result = float(number_text) * multiplier
    return -result if negative and result > 0 else result
