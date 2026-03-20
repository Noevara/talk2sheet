from __future__ import annotations

import io
import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

import pandas as pd

from ..core.schema import HeaderPlan
from ..openai_compatible import OpenAICompatibleError, build_default_llm_client


_PLACEHOLDER_RE = re.compile(r"^(?:unnamed:\s*\d+|列\s*\d+|column\s*\d+)$", re.I)
_COMPACT_DATE_RE = re.compile(r"^\d{4}[-/]\d{1,2}(?:[-/]\d{1,2})?$")
_COMPACT_DATETIME_RE = re.compile(r"^\d{8}(?:\d{4}|\d{6})?$")
_HEADER_ID_LIKE_RE = re.compile(r"^[A-Za-z0-9_.:-]{20,}$")


def _path_cache_key(path: Path) -> tuple[str, int, int]:
    resolved = path.resolve()
    stat = resolved.stat()
    return (str(resolved), int(stat.st_mtime_ns), int(stat.st_size))


def normalize_header_cell(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    lowered = text.lower()
    if lowered in {"nan", "none", "<na>"}:
        return ""
    if _PLACEHOLDER_RE.fullmatch(text):
        return ""
    return re.sub(r"\s+", " ", text)


def merge_header_rows(rows: list[list[Any]]) -> list[str]:
    if not rows:
        return []
    width = max((len(row) for row in rows), default=0)
    merged: list[str] = []
    for column_index in range(width):
        parts: list[str] = []
        seen: set[str] = set()
        for row in rows:
            value = normalize_header_cell(row[column_index] if column_index < len(row) else "")
            if not value:
                continue
            lowered = value.lower()
            if lowered in seen:
                continue
            parts.append(value)
            seen.add(lowered)
        merged.append("/".join(parts))
    return merged


def dedupe_columns(columns: list[str]) -> list[str]:
    out: list[str] = []
    counts: dict[str, int] = {}
    for index, column in enumerate(columns, start=1):
        base = normalize_header_cell(column) or f"Column{index}"
        counts[base] = counts.get(base, 0) + 1
        out.append(base if counts[base] == 1 else f"{base}_{counts[base]}")
    return out


def _data_like_ratio(row: list[Any]) -> float:
    if not row:
        return 0.0
    non_empty = 0
    data_like = 0
    for value in row:
        text = str(value or "").strip()
        if not text:
            continue
        non_empty += 1
        compact = re.sub(r"\s+", "", text)
        numeric_text = compact.replace(",", "").replace("，", "")
        if re.fullmatch(r"[+-]?\d+(?:\.\d+)?", numeric_text):
            data_like += 1
            continue
        if _COMPACT_DATE_RE.fullmatch(compact) or _COMPACT_DATETIME_RE.fullmatch(compact):
            data_like += 1
            continue
    return float(data_like) / float(non_empty) if non_empty else 0.0


def _label_like_ratio(row: list[Any]) -> float:
    if not row:
        return 0.0
    non_empty = 0
    label_like = 0
    for value in row:
        text = normalize_header_cell(value)
        if not text:
            continue
        non_empty += 1
        compact = re.sub(r"\s+", "", text)
        digit_count = sum(1 for ch in compact if ch.isdigit())
        digit_ratio = float(digit_count) / float(max(1, len(compact)))
        if _COMPACT_DATE_RE.fullmatch(compact) or _COMPACT_DATETIME_RE.fullmatch(compact):
            continue
        if _HEADER_ID_LIKE_RE.fullmatch(compact):
            continue
        if len(compact) > 40 and digit_ratio > 0.12:
            continue
        if digit_ratio > 0.35:
            continue
        label_like += 1
    return float(label_like) / float(non_empty) if non_empty else 0.0


def compute_header_health(columns: list[Any]) -> dict[str, Any]:
    normalized = [normalize_header_cell(column) for column in columns]
    total = len(normalized)
    if total == 0:
        return {"total": 0, "invalid": 0, "invalid_ratio": 1.0, "duplicate_count": 0, "non_empty": 0}
    non_empty = [column for column in normalized if column]
    duplicate_count = len(non_empty) - len({column.lower() for column in non_empty})
    invalid = total - len(non_empty)
    return {
        "total": total,
        "invalid": invalid,
        "invalid_ratio": float(invalid) / float(total),
        "duplicate_count": duplicate_count,
        "non_empty": len(non_empty),
    }


def score_header_rows(rows: list[list[Any]], start_index: int, *, depth: int = 1) -> dict[str, Any]:
    header_rows = rows[start_index : start_index + depth]
    merged = merge_header_rows(header_rows)
    health = compute_header_health(merged)
    total = max(1, int(health["total"]))
    non_empty_ratio = float(health["non_empty"]) / float(total)
    duplicate_ratio = float(health["duplicate_count"]) / float(max(1, health["non_empty"]))
    invalid_ratio = float(health["invalid_ratio"])
    header_data_like = sum(_data_like_ratio(row) for row in header_rows) / float(max(1, len(header_rows)))
    next_row = rows[start_index + depth] if (start_index + depth) < len(rows) else []
    next_row_data_like = _data_like_ratio(next_row)
    first_row_non_empty = sum(1 for cell in header_rows[0] if normalize_header_cell(cell)) / float(max(1, total))
    first_row_data_like = _data_like_ratio(header_rows[0])
    first_row_label_like = _label_like_ratio(header_rows[0])
    second_row_non_empty = 0.0
    second_row_data_like = 0.0
    second_row_label_like = 0.0
    if depth == 2 and len(header_rows) > 1:
        second_row_non_empty = sum(1 for cell in header_rows[1] if normalize_header_cell(cell)) / float(max(1, total))
        second_row_data_like = _data_like_ratio(header_rows[1])
        second_row_label_like = _label_like_ratio(header_rows[1])
    score = (
        non_empty_ratio * 0.34
        + (1.0 - invalid_ratio) * 0.22
        + (1.0 - duplicate_ratio) * 0.18
        + (1.0 - header_data_like) * 0.14
        + next_row_data_like * 0.12
        + (
            0.05
            if depth == 2
            and first_row_non_empty >= 0.5
            and second_row_non_empty >= 0.35
            and first_row_data_like < 0.35
            and second_row_data_like < 0.35
            and second_row_label_like >= 0.5
            else 0.0
        )
        - (0.14 if depth == 2 and first_row_non_empty < 0.5 else 0.0)
        - (0.22 if depth == 2 and second_row_data_like >= 0.45 else 0.0)
        - (0.22 if depth == 2 and second_row_label_like < 0.45 else 0.0)
        - (
            0.16
            if depth == 2 and first_row_non_empty >= 0.9 and second_row_non_empty >= 0.9 and first_row_label_like >= 0.85 and second_row_label_like < 0.6
            else 0.0
        )
        - start_index * 0.04
    )
    score = max(0.0, min(1.0, score))
    return {
        "start_index": start_index,
        "start_row_1based": start_index + 1,
        "depth": depth,
        "score": score,
        "headers": merged,
        "health": health,
        "header_data_like_ratio": round(header_data_like, 4),
        "next_row_data_like_ratio": round(next_row_data_like, 4),
        "first_row_label_like_ratio": round(first_row_label_like, 4),
        "second_row_label_like_ratio": round(second_row_label_like, 4),
    }


def detect_header_plan_by_rules(rows: list[list[Any]], *, max_start_rows: int = 6) -> dict[str, Any] | None:
    if not rows:
        return None
    max_index = min(len(rows), max_start_rows)
    candidates: list[dict[str, Any]] = []
    for start_index in range(max_index):
        candidates.append(score_header_rows(rows, start_index, depth=1))
        if start_index + 1 < len(rows):
            candidates.append(score_header_rows(rows, start_index, depth=2))
    if not candidates:
        return None
    candidates.sort(key=lambda item: (item["score"], -item["start_index"], item["depth"]), reverse=True)
    best = candidates[0]
    default = next(
        (candidate for candidate in candidates if candidate["start_index"] == 0 and candidate["depth"] == 1),
        best,
    )
    if best["start_index"] != 0 and float(best["score"]) < float(default["score"]) + 0.08:
        best = default
    has_header = bool(best["health"]["non_empty"])
    if not has_header:
        return {
            "has_header": False,
            "header_row_1based": None,
            "header_depth": 1,
            "data_start_row_1based": 1,
            "confidence": round(float(best["score"]), 4),
            "reason": "rule_based_no_header",
        }
    return {
        "has_header": True,
        "header_row_1based": int(best["start_row_1based"]),
        "header_depth": int(best["depth"]),
        "data_start_row_1based": int(best["start_row_1based"] + best["depth"]),
        "confidence": round(float(best["score"]), 4),
        "reason": "rule_based_header_detection",
        "headers": best["headers"],
        "health": best["health"],
    }


def read_preview_table(
    path: Path,
    *,
    sheet_index: int,
    max_rows: int = 15,
    max_cols: int = 50,
) -> tuple[str, list[list[str]]]:
    sheet_name, cached_rows = _read_preview_table_cached(
        _path_cache_key(path),
        sheet_index=int(sheet_index or 1),
        max_rows=int(max_rows),
        max_cols=int(max_cols),
    )
    return sheet_name, [list(row) for row in cached_rows]


@lru_cache(maxsize=128)
def _read_preview_table_cached(
    cache_key: tuple[str, int, int],
    *,
    sheet_index: int,
    max_rows: int,
    max_cols: int,
) -> tuple[str, tuple[tuple[str, ...], ...]]:
    path = Path(cache_key[0])
    suffix = path.suffix.lower()
    if suffix == ".csv":
        try:
            raw = pd.read_csv(path, header=None, nrows=max_rows)
        except UnicodeDecodeError:
            raw = pd.read_csv(io.StringIO(path.read_text(encoding="utf-8", errors="replace")), header=None, nrows=max_rows)
        rows = tuple(tuple(str(item).strip()[:120] for item in row[:max_cols]) for row in raw.fillna("").values.tolist())
        return path.stem, rows

    if suffix in {".xlsx", ".xlsm", ".xltx", ".xltm", ".xls"}:
        workbook = pd.ExcelFile(path)
        normalized_sheet_index = max(0, min(int(sheet_index or 1) - 1, len(workbook.sheet_names) - 1))
        sheet_name = workbook.sheet_names[normalized_sheet_index]
        raw = pd.read_excel(workbook, sheet_name=sheet_name, header=None, nrows=max_rows)
        rows = tuple(tuple(str(item).strip()[:120] for item in row[:max_cols]) for row in raw.fillna("").values.tolist())
        return sheet_name, rows

    raise RuntimeError(f"Unsupported preview format: {suffix}")


def maybe_detect_header_plan(path: Path, *, sheet_index: int) -> HeaderPlan:
    cached = _maybe_detect_header_plan_cached(_path_cache_key(path), sheet_index=int(sheet_index or 1))
    return HeaderPlan.model_validate(cached)


@lru_cache(maxsize=128)
def _maybe_detect_header_plan_cached(cache_key: tuple[str, int, int], *, sheet_index: int) -> dict[str, Any]:
    path = Path(cache_key[0])
    sheet_name, rows_tuple = _read_preview_table_cached(
        cache_key,
        sheet_index=sheet_index,
        max_rows=15,
        max_cols=50,
    )
    rows = [list(row) for row in rows_tuple]
    rule_plan_dict = detect_header_plan_by_rules(rows) or {
        "has_header": True,
        "header_row_1based": 1,
        "header_depth": 1,
        "data_start_row_1based": 2,
        "confidence": 0.0,
        "reason": "default_header_plan",
    }
    rule_plan = HeaderPlan.model_validate(rule_plan_dict)

    should_try_llm = (
        rule_plan.confidence < 0.82
        and (int(rule_plan.header_row_1based or 1) != 1 or int(rule_plan.header_depth or 1) != 1 or len(rows) > 2)
    )
    if not should_try_llm:
        return rule_plan.model_dump()

    client = build_default_llm_client()
    if not client.enabled:
        return rule_plan.model_dump()

    system_prompt = (
        "You are an Excel header detector / 你是 Excel 表头识别器。 "
        "Return JSON only and follow the HeaderPlan schema exactly. "
        "Decide whether the preview contains a header, where the header starts, and whether it spans one or two rows. "
        "Do not invent row numbers outside the preview window."
    )
    user_prompt = (
        f"sheet_name={sheet_name}\n"
        f"preview_rows={json.dumps(rows, ensure_ascii=False)}\n"
        f"rule_plan={rule_plan.model_dump()}\n"
        "If a header exists, set has_header=true, header_row_1based within 1..15, header_depth within 1..2, "
        "and data_start_row_1based=header_row_1based+header_depth."
    )
    try:
        llm_plan = client.generate_json(
            HeaderPlan,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
        )
    except (OpenAICompatibleError, Exception):
        return rule_plan.model_dump()

    header_row = min(max(int(llm_plan.header_row_1based or 1), 1), max(1, len(rows)))
    header_depth = min(max(int(llm_plan.header_depth or 1), 1), 2)
    return HeaderPlan(
        has_header=bool(llm_plan.has_header),
        header_row_1based=header_row if llm_plan.has_header else None,
        header_depth=header_depth,
        data_start_row_1based=header_row + header_depth if llm_plan.has_header else 1,
        confidence=float(llm_plan.confidence or rule_plan.confidence),
        reason=llm_plan.reason or "llm_header_detection",
    ).model_dump()
