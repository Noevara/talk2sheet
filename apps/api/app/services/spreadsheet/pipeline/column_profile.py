from __future__ import annotations

import re
from typing import Any

import pandas as pd

from ..core.numeric_coercion import coerce_float


HINTS = {
    "amount": ["amount", "price", "cost", "fee", "金额", "费用", "应付", "收入", "支出"],
    "date": ["date", "time", "month", "day", "日期", "时间", "月份", "年月"],
    "id": ["id", "编号", "编码", "单号", "手机号", "邮箱", "email"],
    "name": ["name", "名称", "姓名", "标题"],
    "category": ["type", "category", "标签", "分类", "地区", "区域", "类别"],
}

PROFILE_ATTR = "_talk2sheet_column_profiles"
MIN_NUMERIC_RATIO_FOR_NUMERIC_TYPE = 0.6
MIN_NUMERIC_RATIO_FOR_HINTED_NUMERIC_TYPE = 0.3


def normalize_text(value: str) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[\s_/\\\-:()（）【】\[\],，。.!?]+", "", text)
    return text


def infer_semantic_hints(name: str) -> list[str]:
    lower = str(name or "").lower()
    out: list[str] = []
    for key, tokens in HINTS.items():
        if any(token.lower() in lower for token in tokens):
            out.append(key)
    return out


def build_column_profiles(df: pd.DataFrame) -> dict[str, dict[str, Any]]:
    profiles: dict[str, dict[str, Any]] = {}
    for column in df.columns:
        series = df[column]
        non_null = int(series.notna().sum())
        unique_ratio = float(series.astype(str).nunique(dropna=True)) / float(non_null) if non_null else 0.0

        numeric_count = 0
        sample_values: list[str] = []
        for item in series.dropna().head(20).tolist():
            if len(sample_values) < 5:
                sample_values.append(str(item))
            if coerce_float(item) is not None:
                numeric_count += 1

        numeric_ratio = float(numeric_count) / float(min(non_null, 20)) if non_null else 0.0
        hints = infer_semantic_hints(str(column))

        semantic_type = "text"
        if "date" in hints:
            semantic_type = "date"
        elif numeric_ratio >= MIN_NUMERIC_RATIO_FOR_NUMERIC_TYPE:
            semantic_type = "numeric"
        elif "amount" in hints and numeric_ratio >= MIN_NUMERIC_RATIO_FOR_HINTED_NUMERIC_TYPE:
            semantic_type = "numeric"
        elif "id" in hints or unique_ratio >= 0.95:
            semantic_type = "id"
        elif unique_ratio <= 0.3:
            semantic_type = "categorical"

        profiles[str(column)] = {
            "name": str(column),
            "semantic_type": semantic_type,
            "semantic_hints": hints,
            "unique_ratio": round(unique_ratio, 4),
            "numeric_ratio": round(numeric_ratio, 4),
            "sample_values": sample_values,
            "aliases": [str(column)],
        }
    return profiles


def attach_column_profiles(df: pd.DataFrame, profiles: dict[str, dict[str, Any]] | None = None) -> pd.DataFrame:
    df.attrs[PROFILE_ATTR] = profiles or build_column_profiles(df)
    return df


def get_column_profiles(df: pd.DataFrame) -> dict[str, dict[str, Any]]:
    cached = df.attrs.get(PROFILE_ATTR)
    if isinstance(cached, dict) and cached:
        return cached
    return build_column_profiles(df)
