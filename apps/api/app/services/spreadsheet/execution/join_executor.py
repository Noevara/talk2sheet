from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pandas as pd
from pandas.api.types import is_numeric_dtype

from app.schemas import JoinPreflightResult
from ..analysis.types import AnalysisPayload
from ..analysis.utils import build_execution_disclosure
from ..contracts.workbook_models import WorkbookContext
from ..core.serialization import dataframe_to_rows
from ..pipeline import load_full_dataframe
from ..pipeline.column_profile import attach_column_profiles, get_column_profiles, normalize_text
from ..planning.planner_columns import _find_amount_column, _find_date_column
from ..planning.planner_text_utils import _extract_top_k


_TREND_PATTERNS = (
    re.compile(r"\btrend\b", re.I),
    re.compile(r"趋势", re.I),
)
_TOPN_PATTERNS = (
    re.compile(r"\btop\s*\d*\b", re.I),
    re.compile(r"排行|排名|前\s*\d+", re.I),
)
_AVG_PATTERNS = (
    re.compile(r"\bavg\b", re.I),
    re.compile(r"\baverage\b", re.I),
    re.compile(r"平均", re.I),
)
_COUNT_PATTERNS = (
    re.compile(r"\bcount\b", re.I),
    re.compile(r"数量|多少|几条|几笔", re.I),
)
_SUM_PATTERNS = (
    re.compile(r"\bsum\b", re.I),
    re.compile(r"\btotal\b", re.I),
    re.compile(r"总计|总和|合计|总额|总量", re.I),
)
_CONVERSION_PATTERNS = (
    re.compile(r"\bconversion\b", re.I),
    re.compile(r"转化率|转化", re.I),
)


def _localized(locale: str, *, en: str, zh: str, ja: str) -> str:
    normalized = str(locale or "").lower()
    if normalized.startswith("zh"):
        return zh
    if normalized.startswith("ja"):
        return ja
    return en


def _resolve_sheet_name(workbook_context: WorkbookContext, sheet_index: int) -> str:
    for sheet in workbook_context.sheets:
        if int(sheet.sheet_index or 0) == int(sheet_index):
            return str(sheet.sheet_name or "")
    return f"Sheet {sheet_index}"


def _normalize_key_series(series: pd.Series) -> pd.Series:
    normalized = series.astype("string").str.strip().str.lower()
    normalized = normalized.replace({"": pd.NA, "nan": pd.NA, "none": pd.NA})
    return normalized


def _resolve_key_column(columns: list[str], key_hint: str) -> str:
    normalized_hint = normalize_text(str(key_hint or ""))
    if not normalized_hint:
        return ""
    for column in columns:
        if normalize_text(column) == normalized_hint:
            return str(column)
    for column in columns:
        normalized_col = normalize_text(column)
        if normalized_hint and (normalized_hint in normalized_col or normalized_col in normalized_hint):
            return str(column)
    return ""


def _contains(patterns: tuple[re.Pattern[str], ...], text: str) -> bool:
    return any(pattern.search(text) is not None for pattern in patterns)


def _infer_join_intent(question: str, *, has_metric: bool, has_date: bool) -> str:
    if _contains(_CONVERSION_PATTERNS, question):
        return "conversion"
    if has_date and _contains(_TREND_PATTERNS, question):
        return "trend"
    if _contains(_TOPN_PATTERNS, question):
        return "topn"
    if has_metric and _contains(_AVG_PATTERNS, question):
        return "avg"
    if _contains(_COUNT_PATTERNS, question):
        return "count"
    if has_metric and _contains(_SUM_PATTERNS, question):
        return "sum"
    return "sum" if has_metric else "count"


def _pick_metric_column(df: pd.DataFrame, *, exclude: set[str]) -> str:
    profiles = get_column_profiles(df)
    metric = _find_amount_column(profiles)
    if metric and metric not in exclude:
        return metric
    numeric_columns = [str(col) for col in df.columns if str(col) not in exclude and is_numeric_dtype(df[col])]
    return numeric_columns[0] if numeric_columns else ""


def _pick_date_column(df: pd.DataFrame, *, exclude: set[str]) -> str:
    profiles = get_column_profiles(df)
    date_col = _find_date_column(profiles)
    if date_col and date_col not in exclude:
        return date_col
    for column in df.columns:
        if str(column) in exclude:
            continue
        normalized = normalize_text(str(column))
        if any(token in normalized for token in ("date", "time", "日期", "时间", "month", "day")):
            return str(column)
    return ""


def _pick_dimension_column(df: pd.DataFrame, *, question: str, exclude: set[str]) -> str:
    candidates: list[str] = []
    normalized_question = normalize_text(question)
    for column in df.columns:
        col = str(column)
        if col in exclude:
            continue
        if is_numeric_dtype(df[col]):
            continue
        candidates.append(col)
    if not candidates:
        return ""
    for column in candidates:
        if normalize_text(column) in normalized_question:
            return column
    score_candidates: list[tuple[float, str]] = []
    for column in candidates:
        non_null = df[column].dropna()
        if non_null.empty:
            continue
        unique_ratio = float(non_null.astype("string").nunique(dropna=True)) / float(len(non_null))
        score = 1.0 - abs(unique_ratio - 0.2)
        score_candidates.append((score, column))
    if score_candidates:
        score_candidates.sort(reverse=True)
        return score_candidates[0][1]
    return candidates[0]


def _build_answer(
    *,
    locale: str,
    intent: str,
    join_type: str,
    join_key: str,
    left_name: str,
    right_name: str,
    matched_rows: int,
    total_rows: int,
    result_df: pd.DataFrame,
    metric_column: str,
    dimension_column: str,
) -> str:
    if intent == "conversion":
        ratio = 0.0
        if total_rows > 0:
            ratio = float(matched_rows) / float(total_rows)
        return _localized(
            locale,
            en=(
                f"Join Beta executed ({join_type}) on key '{join_key}' between '{left_name}' and '{right_name}'. "
                f"Matched rows: {matched_rows}/{total_rows} ({ratio:.2%})."
            ),
            zh=(
                f"Join Beta 已执行（{join_type}），键「{join_key}」，来源「{left_name} + {right_name}」。"
                f"匹配行数：{matched_rows}/{total_rows}（{ratio:.2%}）。"
            ),
            ja=(
                f"Join Beta を実行しました（{join_type}、キー: {join_key}、{left_name} + {right_name}）。"
                f"一致行: {matched_rows}/{total_rows}（{ratio:.2%}）。"
            ),
        )

    if intent in {"sum", "avg", "count"} and not result_df.empty:
        value = result_df.iloc[0][result_df.columns[-1]]
        return _localized(
            locale,
            en=f"Join Beta result ({join_type}, key={join_key}): {intent} = {value}.",
            zh=f"Join Beta 结果（{join_type}，键={join_key}）：{intent} = {value}。",
            ja=f"Join Beta 結果（{join_type}、キー={join_key}）：{intent} = {value}。",
        )

    if intent == "trend":
        return _localized(
            locale,
            en=f"Join Beta trend generated using '{metric_column}' over time.",
            zh=f"Join Beta 已生成趋势结果（指标「{metric_column}」）。",
            ja=f"Join Beta のトレンド結果を生成しました（指標: {metric_column}）。",
        )

    if intent == "topn":
        return _localized(
            locale,
            en=f"Join Beta top list generated by '{dimension_column}' using '{metric_column or 'count'}'.",
            zh=f"Join Beta 已生成 Top 结果（维度「{dimension_column}」，指标「{metric_column or 'count'}」）。",
            ja=f"Join Beta の Top 結果を生成しました（次元: {dimension_column}、指標: {metric_column or 'count'}）。",
        )

    return _localized(
        locale,
        en=f"Join Beta executed ({join_type}) on key '{join_key}'.",
        zh=f"Join Beta 已执行（{join_type}），键「{join_key}」。",
        ja=f"Join Beta を実行しました（{join_type}、キー: {join_key}）。",
    )


def _aggregate_join_result(
    *,
    joined: pd.DataFrame,
    question: str,
    locale: str,
    key_columns: set[str],
) -> tuple[pd.DataFrame, str, str, str]:
    attach_column_profiles(joined)
    metric_col = _pick_metric_column(joined, exclude=key_columns | {"_merge"})
    date_col = _pick_date_column(joined, exclude=key_columns | {"_merge"})
    intent = _infer_join_intent(question, has_metric=bool(metric_col), has_date=bool(date_col))

    if intent == "conversion":
        total = int(len(joined.index))
        matched = int((joined["_merge"] != "left_only").sum()) if "_merge" in joined.columns else total
        ratio = float(matched) / float(max(total, 1))
        result_df = pd.DataFrame(
            [
                {
                    "matched_rows": matched,
                    "total_rows": total,
                    "conversion_rate": round(ratio, 4),
                }
            ]
        )
        return result_df, intent, metric_col, ""

    if intent == "count":
        result_df = pd.DataFrame([{"count": int(len(joined.index))}])
        return result_df, intent, metric_col, ""

    if intent == "avg" and metric_col:
        value = float(pd.to_numeric(joined[metric_col], errors="coerce").mean())
        result_df = pd.DataFrame([{"average": round(value, 6)}])
        return result_df, intent, metric_col, ""

    if intent == "trend" and date_col:
        date_series = pd.to_datetime(joined[date_col], errors="coerce")
        filtered = joined[date_series.notna()].copy()
        filtered["period"] = date_series[date_series.notna()].dt.to_period("M").astype(str)
        if metric_col:
            filtered["__metric__"] = pd.to_numeric(filtered[metric_col], errors="coerce").fillna(0.0)
            grouped = filtered.groupby("period", dropna=False)["__metric__"].sum().reset_index(name="value")
            grouped.rename(columns={"period": "month"}, inplace=True)
            result_df = grouped.sort_values("month", ascending=True).reset_index(drop=True)
        else:
            grouped = filtered.groupby("period", dropna=False).size().reset_index(name="count")
            grouped.rename(columns={"period": "month"}, inplace=True)
            result_df = grouped.sort_values("month", ascending=True).reset_index(drop=True)
        return result_df, intent, metric_col, "month"

    if intent == "topn":
        top_k = _extract_top_k(question, default=5, upper=20)
        dimension_col = _pick_dimension_column(joined, question=question, exclude=key_columns | {"_merge"})
        if not dimension_col:
            dimension_col = "__join_key_norm"
            joined[dimension_col] = joined[dimension_col].astype("string")
        if metric_col:
            joined["__metric__"] = pd.to_numeric(joined[metric_col], errors="coerce").fillna(0.0)
            grouped = joined.groupby(dimension_col, dropna=False)["__metric__"].sum().reset_index(name="value")
        else:
            grouped = joined.groupby(dimension_col, dropna=False).size().reset_index(name="count")
        result_df = grouped.sort_values(result_df_col := grouped.columns[-1], ascending=False).head(top_k).reset_index(drop=True)
        return result_df, intent, metric_col, dimension_col

    if metric_col:
        value = float(pd.to_numeric(joined[metric_col], errors="coerce").sum())
        result_df = pd.DataFrame([{"sum": round(value, 6)}])
        return result_df, "sum", metric_col, ""

    result_df = pd.DataFrame([{"count": int(len(joined.index))}])
    return result_df, "count", metric_col, ""


def execute_join_beta(
    *,
    path: Path,
    workbook_context: WorkbookContext,
    preflight: JoinPreflightResult,
    question: str,
    requested_mode: str,
    locale: str,
) -> AnalysisPayload:
    sheet_indexes = [int(item) for item in list(preflight.sheet_indexes or []) if int(item) > 0][:2]
    if len(sheet_indexes) != 2:
        raise ValueError("join beta requires exactly two sheet indexes")

    left_index, right_index = sheet_indexes[0], sheet_indexes[1]
    left_name = _resolve_sheet_name(workbook_context, left_index)
    right_name = _resolve_sheet_name(workbook_context, right_index)

    left_df, _ = load_full_dataframe(path, sheet_index=left_index)
    right_df, _ = load_full_dataframe(path, sheet_index=right_index)

    key_hint = str(preflight.join_key or "").strip()
    left_key_col = _resolve_key_column([str(col) for col in left_df.columns], str(preflight.left_sheet.key_column if preflight.left_sheet else key_hint))
    right_key_col = _resolve_key_column([str(col) for col in right_df.columns], str(preflight.right_sheet.key_column if preflight.right_sheet else key_hint))
    if not left_key_col or not right_key_col:
        raise ValueError("join beta could not resolve key columns")

    left = left_df.copy()
    right = right_df.copy()
    left["__join_key_norm"] = _normalize_key_series(left[left_key_col])
    right["__join_key_norm"] = _normalize_key_series(right[right_key_col])
    left = left[left["__join_key_norm"].notna()].copy()
    right = right[right["__join_key_norm"].notna()].copy()
    left_key_set = set(left["__join_key_norm"].astype("string").tolist())
    right_key_set = set(right["__join_key_norm"].astype("string").tolist())
    left_unmatched_rows = int((~left["__join_key_norm"].isin(right_key_set)).sum())
    right_unmatched_rows = int((~right["__join_key_norm"].isin(left_key_set)).sum())
    matched_rows = int(len(left.index) - left_unmatched_rows)
    match_rate = float(matched_rows) / float(max(len(left.index), 1))

    join_type = str(preflight.join_type or "inner").strip().lower()
    if join_type not in {"inner", "left"}:
        join_type = "inner"
    merged = left.merge(
        right,
        how=join_type,
        on="__join_key_norm",
        suffixes=(f" ({left_name})", f" ({right_name})"),
        indicator=True,
    )
    if len(merged.index) > 200000:
        raise ValueError("join beta result too large")

    aggregate_df, intent, metric_col, dimension_col = _aggregate_join_result(
        joined=merged.copy(),
        question=question,
        locale=locale,
        key_columns={left_key_col, right_key_col, "__join_key_norm"},
    )
    answer = _build_answer(
        locale=locale,
        intent=intent,
        join_type=join_type,
        join_key=key_hint or left_key_col,
        left_name=left_name,
        right_name=right_name,
        matched_rows=matched_rows,
        total_rows=int(len(left.index)),
        result_df=aggregate_df,
        metric_column=metric_col,
        dimension_column=dimension_col,
    )

    preview_df = aggregate_df.head(50).copy()
    answer_segments = {
        "conclusion": answer,
        "evidence": _localized(
            locale,
            en=f"Join rows={len(merged.index)}, matched_left={matched_rows}, match_rate={match_rate:.2%}.",
            zh=f"Join 行数={len(merged.index)}，左表匹配行={matched_rows}，匹配率={match_rate:.2%}。",
            ja=f"Join 行数={len(merged.index)}、左シート一致行={matched_rows}、一致率={match_rate:.2%}。",
        ),
        "risk_note": _localized(
            locale,
            en=f"Unmatched rows: left={left_unmatched_rows}, right={right_unmatched_rows}.",
            zh=f"未匹配行：左表={left_unmatched_rows}，右表={right_unmatched_rows}。",
            ja=f"未一致行: 左={left_unmatched_rows}、右={right_unmatched_rows}。",
        ),
    }

    pipeline: dict[str, Any] = {
        "status": "ok",
        "planner": {
            "provider": "join_beta_executor",
            "intent": f"join_beta_{intent}",
            "analysis_intent": {
                "kind": f"join_beta_{intent}",
                "join_requested": True,
                "join_key": key_hint or left_key_col,
                "join_type": join_type,
                "join_beta_eligible": True,
                "join_gate_reasons": [],
            },
        },
        "join_beta": {
            "enabled": True,
            "executed": True,
            "intent": intent,
            "join_type": join_type,
            "join_key": key_hint or left_key_col,
            "sheet_indexes": sheet_indexes,
            "left_sheet_index": left_index,
            "left_sheet_name": left_name,
            "right_sheet_index": right_index,
            "right_sheet_name": right_name,
            "left_rows": int(len(left_df.index)),
            "right_rows": int(len(right_df.index)),
            "joined_rows": int(len(merged.index)),
            "matched_rows": int(matched_rows),
            "left_unmatched_rows": int(left_unmatched_rows),
            "right_unmatched_rows": int(right_unmatched_rows),
            "match_rate": round(match_rate, 4),
            "preflight": preflight.model_dump(),
        },
        "result_columns": [str(column) for column in preview_df.columns],
        "result_row_count": int(len(aggregate_df.index)),
        "preview_rows": dataframe_to_rows(preview_df),
        "answer_generation": {
            "provider_used": "join_beta_rule_based",
            "summary_kind": "join_beta",
            "segments": answer_segments,
        },
        "source_sheet_index": left_index,
        "source_sheet_name": f"{left_name} + {right_name}",
    }
    execution_disclosure = build_execution_disclosure(
        locale,
        rows_loaded=max(int(len(left_df.index)), int(len(right_df.index))),
        exact_used=True,
        fallback_reason="",
    )
    return AnalysisPayload(
        mode="text",
        answer=answer,
        analysis_text=answer,
        pipeline=pipeline,
        execution_disclosure=execution_disclosure,
        chart_spec=None,
        chart_data=None,
    )
