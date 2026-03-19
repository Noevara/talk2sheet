from __future__ import annotations

import math
from typing import Any

import pandas as pd

from app.schemas import ExecutionDisclosure
from ..core.i18n import pick_locale
from ..core.schema import ChartSpec, TransformPlan
from ..planning.intent_accessors import analysis_intent_payload
from .answer_models import GeneratedAnswer
from .templates import _ta


def _is_blank(value: Any) -> bool:
    if value is None:
        return True
    try:
        if bool(pd.isna(value)):
            return True
    except Exception:
        pass
    return isinstance(value, str) and not value.strip()


def _format_number(value: Any) -> str:
    try:
        numeric = float(value)
    except Exception:
        return str(value)
    if math.isnan(numeric) or math.isinf(numeric):
        return "0"
    rounded = round(numeric)
    if abs(numeric - rounded) < 1e-9:
        return f"{int(rounded):,}"
    return f"{numeric:,.4f}".rstrip("0").rstrip(".")


def _format_value(value: Any) -> str:
    if _is_blank(value):
        return "-"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return _format_number(value)
    return str(value)


def _rank_label(locale: str, rank_position: int) -> str:
    if rank_position == -1:
        if locale == "zh-CN":
            return "最后一名"
        if locale == "ja-JP":
            return "最下位"
        return "last"
    if locale == "zh-CN":
        return f"第{rank_position}名"
    if locale == "ja-JP":
        return f"{rank_position}位"
    suffix = "th"
    if rank_position % 100 not in {11, 12, 13}:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(rank_position % 10, "th")
    return f"{rank_position}{suffix}"


def _safe_scalar(df: pd.DataFrame, column: str | None = None) -> Any:
    if df.empty:
        return None
    if column and column in df.columns:
        return df.iloc[0][column]
    return df.iloc[0][0] if len(df.columns) else None


def _column_candidates(plan: TransformPlan) -> list[str]:
    candidates: list[str] = []
    for metric in plan.metrics or []:
        if metric.as_name:
            candidates.append(metric.as_name)
        if metric.col:
            candidates.append(metric.col)
    for metric in plan.formula_metrics or []:
        candidates.append(metric.as_name)
    for metric in plan.post_pivot_formula_metrics or []:
        candidates.append(metric.as_name)
    if plan.order_by is not None:
        candidates.append(plan.order_by.col)
    return candidates


def _metric_column(df: pd.DataFrame, *, plan: TransformPlan, chart_spec: ChartSpec | None) -> str | None:
    candidates: list[str] = []
    if chart_spec is not None and chart_spec.y:
        candidates.append(chart_spec.y)
    candidates.extend(_column_candidates(plan))
    seen: set[str] = set()
    for candidate in candidates:
        if candidate in df.columns and candidate not in seen:
            seen.add(candidate)
            return candidate
    numeric_columns = [str(column) for column in df.columns if pd.api.types.is_numeric_dtype(df[column])]
    if numeric_columns:
        return numeric_columns[0]
    return str(df.columns[-1]) if len(df.columns) else None


def _dimension_column(df: pd.DataFrame, *, plan: TransformPlan, chart_spec: ChartSpec | None, value_column: str | None) -> str | None:
    candidates: list[str] = []
    if chart_spec is not None and chart_spec.x:
        candidates.append(chart_spec.x)
    candidates.extend(plan.groupby or [])
    if plan.pivot is not None:
        candidates.extend(plan.pivot.index or [])
    seen: set[str] = set()
    for candidate in candidates:
        if candidate in df.columns and candidate not in seen:
            seen.add(candidate)
            return candidate
    for column in [str(item) for item in df.columns]:
        if column != value_column:
            return column
    return None


def _metric_label(plan: TransformPlan, *, fallback: str = "value") -> str:
    for metric in plan.metrics or []:
        if metric.col:
            return metric.col
        if metric.as_name:
            return metric.as_name
    for metric in plan.formula_metrics or []:
        if metric.as_name:
            return metric.as_name
    for metric in plan.post_pivot_formula_metrics or []:
        if metric.as_name:
            return metric.as_name
    return fallback


def _row_summary(df: pd.DataFrame, *, preferred_columns: list[str] | None = None, limit: int = 4) -> str:
    if df.empty:
        return "-"
    row = df.iloc[0]
    columns: list[str] = []
    for column in preferred_columns or []:
        if column in df.columns and column not in columns:
            columns.append(column)
    for column in [str(item) for item in df.columns]:
        if column not in columns:
            columns.append(column)

    parts: list[str] = []
    for column in columns:
        value = _format_value(row[column])
        if value == "-":
            continue
        parts.append(f"{column}={value}")
        if len(parts) >= limit:
            break
    return ", ".join(parts) if parts else "-"


def _columns_summary(df: pd.DataFrame, *, limit: int = 6) -> str:
    columns = [str(column) for column in df.columns]
    if len(columns) <= limit:
        return ", ".join(columns)
    return ", ".join(columns[:limit]) + ", ..."


def _top_items_summary(df: pd.DataFrame, *, dimension_column: str, value_column: str, limit: int) -> str:
    if df.empty or dimension_column not in df.columns or value_column not in df.columns:
        return "-"
    parts: list[str] = []
    for _, row in df.head(limit).iterrows():
        parts.append(f"{_format_value(row[dimension_column])}（{_format_number(row[value_column])}）")
    return "、".join(parts) if parts else "-"


def _chart_clause(locale: str, chart_spec: ChartSpec | None) -> str:
    if chart_spec is None:
        return ""
    return _ta(locale, "chart_clause", chart_type=_ta(locale, f"chart_type_{chart_spec.type}"))


def _trend_change_clause(locale: str, values: pd.Series) -> str:
    clean = values.dropna()
    if len(clean.index) < 2:
        return ""
    delta = float(clean.iloc[-1]) - float(clean.iloc[0])
    if abs(delta) < 1e-9:
        return _ta(locale, "trend_change_flat")
    key = "trend_change_up" if delta > 0 else "trend_change_down"
    return _ta(locale, key, delta_value=_format_number(abs(delta)))


def _looks_amount_column(column: str) -> bool:
    lowered = str(column or "").lower()
    return any(token in lowered for token in ("amount", "cost", "fee", "price", "消费", "费用", "金额", "应付", "payable"))


def _looks_date_column(column: str) -> bool:
    lowered = str(column or "").lower()
    return any(token in lowered for token in ("date", "time", "month", "day", "日期", "时间", "月份"))


def _summary_sentence_for_explain(context: Any, *, target: str, dimension: str) -> str:
    df = context.result_df
    if df.empty:
        return ""

    amount_column: str | None = None
    for column in [str(item) for item in df.columns]:
        if _looks_amount_column(column):
            amount_column = column
            break
    if amount_column is None:
        for column in [str(item) for item in df.columns]:
            if pd.api.types.is_numeric_dtype(df[column]):
                amount_column = column
                break

    date_column = next((str(column) for column in df.columns if _looks_date_column(str(column))), None)
    detail_dimension = None
    for column in [str(item) for item in df.columns]:
        if column in {dimension, amount_column, date_column}:
            continue
        if pd.api.types.is_numeric_dtype(df[column]):
            continue
        detail_dimension = column
        break

    parts: list[str] = []
    if amount_column and amount_column in df.columns:
        values = pd.to_numeric(df[amount_column], errors="coerce").dropna()
        if not values.empty:
            total_value = _format_number(values.sum())
            max_value = _format_number(values.max())
            if context.locale == "zh-CN":
                parts.append(f"相关记录合计 {total_value}，其中单条最高为 {max_value}。")
            elif context.locale == "ja-JP":
                parts.append(f"関連行の合計は {total_value}、単一行の最大は {max_value} です。")
            else:
                parts.append(f"The matching rows sum to {total_value}, and the largest single row is {max_value}.")
    if date_column and date_column in df.columns:
        normalized = df[date_column].astype(str).replace({"nan": ""})
        non_empty = [item for item in normalized.tolist() if item]
        if non_empty:
            start_value = non_empty[0]
            end_value = non_empty[-1]
            if start_value != end_value:
                if context.locale == "zh-CN":
                    parts.append(f"时间范围覆盖 {start_value} 到 {end_value}。")
                elif context.locale == "ja-JP":
                    parts.append(f"期間は {start_value} から {end_value} です。")
                else:
                    parts.append(f"The time span runs from {start_value} to {end_value}.")
    if detail_dimension and amount_column and detail_dimension in df.columns:
        grouped = (
            df[[detail_dimension, amount_column]]
            .assign(**{amount_column: pd.to_numeric(df[amount_column], errors="coerce")})
            .dropna(subset=[amount_column])
            .groupby(detail_dimension, dropna=False)[amount_column]
            .sum()
            .sort_values(ascending=False)
        )
        if not grouped.empty:
            leader_label = _format_value(grouped.index[0])
            leader_value = _format_number(grouped.iloc[0])
            if context.locale == "zh-CN":
                parts.append(f"按 {detail_dimension} 看，{leader_label} 贡献最高，为 {leader_value}。")
            elif context.locale == "ja-JP":
                parts.append(f"{detail_dimension} 別では {leader_label} が最大で、{leader_value} です。")
            else:
                parts.append(f"By {detail_dimension}, {leader_label} contributes the most at {leader_value}.")

    return " ".join(parts).strip()


def _period_compare_direction(locale: str, delta: float) -> str:
    if abs(delta) < 1e-9:
        if locale == "zh-CN":
            return "持平 "
        if locale == "ja-JP":
            return "横ばいで "
        return "flat by "
    if delta > 0:
        if locale == "zh-CN":
            return "上升 "
        if locale == "ja-JP":
            return "増加 "
        return "up "
    if locale == "zh-CN":
        return "下降 "
    if locale == "ja-JP":
        return "減少 "
    return "down "


def _normalize_segment(text: Any) -> str:
    return " ".join(str(text or "").split()).strip()


def _compose_analysis_text(*parts: Any) -> str:
    out: list[str] = []
    seen: set[str] = set()
    for part in parts:
        text = _normalize_segment(part)
        if not text or text in seen:
            continue
        out.append(text)
        seen.add(text)
    return " ".join(out)


def _risk_note_from_disclosure(disclosure: ExecutionDisclosure | None) -> str:
    if disclosure is None:
        return ""
    parts: list[str] = []
    if not disclosure.exact_used and disclosure.scope_text:
        parts.append(str(disclosure.scope_text))
    if disclosure.fallback_reason:
        parts.append(str(disclosure.fallback_reason))
    return _compose_analysis_text(*parts)


def _forecast_model_label(locale: str, model: str) -> str:
    normalized = str(model or "").strip().lower()
    labels = {
        "linear_regression": {"en": "linear regression", "zh-CN": "线性回归趋势", "ja-JP": "線形回帰"},
        "simple_exponential_smoothing": {"en": "simple exponential smoothing", "zh-CN": "简单指数平滑", "ja-JP": "単純指数平滑"},
    }
    selected = labels.get(normalized, {})
    return str(selected.get(pick_locale(locale)) or normalized or model)


def _forecast_grain_label(locale: str, grain: str) -> str:
    normalized = str(grain or "").strip().lower()
    labels = {
        "day": {"en": "day", "zh-CN": "天", "ja-JP": "日"},
        "week": {"en": "week", "zh-CN": "周", "ja-JP": "週"},
        "month": {"en": "month", "zh-CN": "月", "ja-JP": "月"},
    }
    selected = labels.get(normalized, {})
    return str(selected.get(pick_locale(locale)) or normalized or grain)


def _finalize_generated_answer(
    *,
    answer: str,
    analysis_text: str,
    meta: dict[str, Any] | None = None,
    conclusion: str = "",
    evidence: str = "",
    risk_note: str = "",
) -> GeneratedAnswer:
    normalized_conclusion = _normalize_segment(conclusion) or _normalize_segment(answer)
    normalized_evidence = _normalize_segment(evidence)
    if not normalized_evidence:
        candidate = _normalize_segment(analysis_text)
        if candidate and candidate != normalized_conclusion:
            normalized_evidence = candidate
    normalized_risk_note = _normalize_segment(risk_note)
    final_analysis_text = _compose_analysis_text(normalized_evidence, normalized_risk_note) or _normalize_segment(analysis_text) or normalized_conclusion
    segments = {
        "conclusion": normalized_conclusion,
        "evidence": normalized_evidence,
        "risk_note": normalized_risk_note,
    }
    payload_meta = dict(meta or {})
    payload_meta["segments"] = segments
    return GeneratedAnswer(
        answer=normalized_conclusion,
        analysis_text=final_analysis_text,
        meta=payload_meta,
        segments=segments,
    )


def _locale_language(locale: str) -> str:
    selected = pick_locale(locale)
    if selected == "zh-CN":
        return "Simplified Chinese"
    if selected == "ja-JP":
        return "Japanese"
    return "English"


def _preview_csv(df: pd.DataFrame, *, limit: int = 8) -> str:
    if df.empty:
        return ""
    preview = df.head(limit).copy()
    return preview.fillna("").to_csv(index=False)


def _followup_summary(followup_context: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(followup_context, dict):
        return {}
    last_turn = followup_context.get("last_turn")
    out = {
        "conversation_id": followup_context.get("conversation_id"),
        "turn_count": int(followup_context.get("turn_count") or 0),
        "is_followup": bool(followup_context.get("is_followup")),
        "last_mode": str(followup_context.get("last_mode") or ""),
        "last_pipeline_summary": followup_context.get("last_pipeline_summary") if isinstance(followup_context.get("last_pipeline_summary"), dict) else {},
        "last_result_columns": [str(item) for item in (followup_context.get("last_result_columns") or [])],
        "last_result_row_count": int(followup_context.get("last_result_row_count") or 0),
    }
    if isinstance(last_turn, dict):
        out["last_turn"] = {
            "question": str(last_turn.get("question") or ""),
            "intent": str(last_turn.get("intent") or ""),
            "analysis_intent": analysis_intent_payload(last_turn),
            "answer_summary": str(last_turn.get("answer_summary") or ""),
        }
    history = followup_context.get("recent_pipeline_history")
    if isinstance(history, list):
        out["recent_pipeline_history"] = history[:3]
    return out
