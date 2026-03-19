from __future__ import annotations

import re
from typing import Any

from ..core.schema import Clarification
from ..pipeline.column_profile import normalize_text
from .intent_models import AnalysisIntent, AnalysisTimeScope
from .planner_intent_signals import (
    _average_amount_question,
    _compare_question,
    _distinct_question,
    _forecast_question,
    _ranking_question,
    _row_count_question,
    _share_question,
    _total_amount_question,
    _trend_question,
    _weekday_weekend_question,
)
from .planner_types import ResolvedColumns


def _name_like_candidates(
    profiles: dict[str, dict[str, Any]],
    columns: ResolvedColumns,
) -> list[tuple[str, str]]:
    candidates: list[tuple[str, str]] = []
    seen: set[str] = set()

    def add(column: str | None, role: str) -> None:
        if not column or column in seen:
            return
        normalized = normalize_text(column)
        if "name" not in normalized and "名称" not in column:
            return
        seen.add(column)
        candidates.append((column, role))

    add(columns.service_column, "service")
    add(columns.item_preferred_column, "item")
    add(columns.item_column, "item")

    for column in profiles:
        normalized = normalize_text(column)
        if column in seen:
            continue
        if "name" in normalized or "名称" in column:
            seen.add(column)
            candidates.append((column, "dimension"))

    return candidates


def _generic_name_reference(question: str) -> str | None:
    text = str(question or "")
    lowered = text.lower()

    if any(token in text for token in ("服务名称", "计费项名称", "项目名称", "商品名称", "产品名称", "资源名称", "地区名称", "地域名称")):
        return None
    if any(token in lowered for token in ("service name", "billing item name", "item name", "product name", "region name")):
        return None
    if "名称" in text:
        return "名称"
    if re.search(r"\bname\b", lowered):
        return "name"
    return None


def _build_intent_clarification(
    *,
    question: str,
    profiles: dict[str, dict[str, Any]],
    columns: ResolvedColumns,
    followup_context: dict[str, Any] | None,
) -> Clarification | None:
    if isinstance(followup_context, dict) and isinstance(followup_context.get("clarification_resolution"), dict):
        return None

    source_field = _generic_name_reference(question)
    if source_field is None:
        return None

    candidates = _name_like_candidates(profiles, columns)
    if len(candidates) < 2:
        return None

    reason = (
        f"问题里的“{source_field}”可能对应多个列，请先确认要按哪个维度分析。"
        if source_field != "name"
        else 'The term "name" matches multiple columns. Choose the dimension first.'
    )
    return Clarification(
        reason=reason,
        field=source_field,
        options=[
            {
                "label": column,
                "value": column,
                "semantic_role": role,
            }
            for column, role in candidates
        ],
    )


def _infer_kind(question: str, *, draft: Any | None = None) -> str:
    if draft is not None:
        legacy_intent = str(getattr(draft, "intent", "") or "").strip()
        if legacy_intent:
            return legacy_intent

    if _forecast_question(question):
        return "forecast_timeseries"
    if _weekday_weekend_question(question):
        return "weekpart_compare"
    if _share_question(question):
        return "share"
    if _compare_question(question):
        return "period_compare"
    if _trend_question(question):
        return "trend"
    if _ranking_question(question):
        return "ranking"
    if _row_count_question(question):
        return "row_count"
    if _distinct_question(question):
        return "count_distinct"
    if _average_amount_question(question):
        return "average_amount"
    if _total_amount_question(question):
        return "total_amount"
    return "unsupported"


def _infer_target_metric(
    *,
    kind: str,
    question: str,
    amount_column: str | None,
    draft: Any | None = None,
) -> str | None:
    if kind == "active_day_count":
        return "count_distinct_day"
    if kind == "forecast_timeseries":
        return amount_column
    if kind in {"ranked_item_lookup", "explain_ranked_item", "detail_rows"}:
        return amount_column
    if draft is not None:
        planner_meta = dict(getattr(draft, "planner_meta", {}) or {})
        if kind == "what_if_reduction":
            return str(planner_meta.get("what_if_target_column") or amount_column or "reduction_amount").strip() or None
        if kind == "period_compare":
            return str(planner_meta.get("compare_metric_column") or amount_column or "value").strip() or None
        if kind == "explain_breakdown":
            return str(planner_meta.get("breakdown_metric_column") or amount_column or "value").strip() or None

    if draft is not None:
        metrics = list(getattr(getattr(draft, "transform_plan", None), "metrics", []) or [])
        if metrics:
            metric = metrics[0]
            agg = str(getattr(metric, "agg", "") or "")
            if agg == "sum":
                return amount_column or str(getattr(metric, "col", "") or "sum")
            if agg == "avg":
                return amount_column or str(getattr(metric, "col", "") or "avg")
            if agg in {"count_rows", "count_distinct", "nunique"}:
                return agg

    if _row_count_question(question):
        return "row_count"
    if _distinct_question(question):
        return "count_distinct"
    if _average_amount_question(question):
        return amount_column
    if _total_amount_question(question) or _ranking_question(question) or _share_question(question) or _trend_question(question):
        return amount_column
    return None


def _infer_target_dimension(
    *,
    kind: str,
    columns: ResolvedColumns,
    draft: Any | None = None,
) -> str | None:
    if kind not in {
        "ranking",
        "share",
        "trend",
        "period_compare",
        "period_breakdown",
        "weekpart_compare",
        "detail_rows",
        "count_distinct",
        "explain_ranked_item",
        "ranked_item_lookup",
        "explain_breakdown",
        "what_if_reduction",
    }:
        return None

    if draft is not None:
        planner_meta = dict(getattr(draft, "planner_meta", {}) or {})
        for key in (
            "ranking_column",
            "share_column",
            "distinct_column",
            "explain_dimension_column",
            "ranked_item_dimension_column",
            "breakdown_dimension",
            "breakdown_target_dimension",
            "what_if_target_column",
        ):
            value = str(planner_meta.get(key) or "").strip()
            if value:
                return value

        transform_plan = getattr(draft, "transform_plan", None)
        groupby = list(getattr(transform_plan, "groupby", []) or [])
        if groupby:
            return str(groupby[0] or "")

        chart_spec = getattr(draft, "chart_spec", None)
        chart_x = str(getattr(chart_spec, "x", "") or "").strip()
        if chart_x:
            return chart_x

    return columns.question_dimension_column or columns.category_column


def _infer_comparison_type(kind: str) -> str | None:
    mapping = {
        "ranking": "ranking",
        "share": "share",
        "trend": "trend",
        "period_compare": "period_compare",
        "period_breakdown": "period_breakdown",
        "weekpart_compare": "period_compare",
        "forecast_timeseries": "forecast",
        "detail_rows": "detail",
        "explain_ranked_item": "explain",
        "explain_breakdown": "breakdown",
        "ranked_item_lookup": "rank_lookup",
        "what_if_reduction": "simulation",
        "active_day_count": "count_distinct_time",
    }
    return mapping.get(kind)


def _infer_time_scope(
    *,
    question: str,
    followup_context: dict[str, Any] | None,
    draft: Any | None = None,
) -> AnalysisTimeScope | None:
    grain: str | None = None
    requested_period = ""
    requested_periods: list[str] = []

    if draft is not None:
        planner_meta = dict(getattr(draft, "planner_meta", {}) or {})
        requested_period = str(planner_meta.get("requested_period") or planner_meta.get("forecast_target_period") or "")
        raw_requested_periods = planner_meta.get("requested_months") or planner_meta.get("forecast_target_periods") or []
        requested_periods = [str(item) for item in raw_requested_periods if str(item or "").strip()]

        transform_plan = getattr(draft, "transform_plan", None)
        derived_columns = list(getattr(transform_plan, "derived_columns", []) or [])
        for derived in derived_columns:
            derived_grain = str(getattr(derived, "grain", "") or "").strip()
            if derived_grain:
                grain = derived_grain
                break
        if grain is None:
            grain = str(planner_meta.get("bucket_grain") or planner_meta.get("forecast_grain") or planner_meta.get("compare_grain") or "").strip() or None
    else:
        if _trend_question(question) or _compare_question(question) or _forecast_question(question):
            grain = "month"

    if grain is None and not requested_period and not requested_periods and not isinstance(followup_context, dict):
        return None

    return AnalysisTimeScope(
        grain=grain,
        requested_period=requested_period or None,
        requested_periods=requested_periods,
        is_followup=bool((followup_context or {}).get("is_followup")) if isinstance(followup_context, dict) else False,
    )


def _infer_answer_expectation(*, kind: str, mode: str, draft: Any | None = None, clarification: Clarification | None = None) -> str:
    if clarification is not None:
        return "clarification"
    if kind == "unsupported":
        return "unsupported"
    if kind in {"row_count", "count_distinct", "total_amount", "average_amount", "active_day_count", "what_if_reduction", "ranked_item_lookup"}:
        return "single_value"
    if kind in {"detail_rows", "explain_ranked_item"}:
        return "detail_rows"
    if kind in {"period_compare", "period_breakdown", "ranking", "share", "trend", "weekpart_compare", "explain_breakdown", "forecast_timeseries"} and mode == "chart":
        return "chart"
    if kind in {"period_compare", "period_breakdown", "ranking", "share", "trend", "weekpart_compare", "explain_breakdown", "forecast_timeseries"}:
        return "summary_table"
    if draft is not None and bool(getattr(getattr(draft, "transform_plan", None), "return_rows", False)):
        return "detail_rows"
    if mode == "chart" or getattr(draft, "chart_spec", None) is not None:
        return "chart"
    if draft is not None:
        transform_plan = getattr(draft, "transform_plan", None)
        metrics = list(getattr(transform_plan, "metrics", []) or [])
        groupby = list(getattr(transform_plan, "groupby", []) or [])
        if metrics and not groupby:
            return "single_value"
        if groupby:
            return "summary_table"
    return "unsupported" if mode == "unsupported" else "summary_table"


def understand_analysis_intent(
    *,
    question: str,
    mode: str,
    profiles: dict[str, dict[str, Any]],
    columns: ResolvedColumns,
    followup_context: dict[str, Any] | None = None,
    draft: Any | None = None,
) -> AnalysisIntent:
    clarification = _build_intent_clarification(
        question=question,
        profiles=profiles,
        columns=columns,
        followup_context=followup_context,
    )
    kind = _infer_kind(question, draft=draft)

    return AnalysisIntent(
        kind=kind,
        legacy_intent=str(getattr(draft, "intent", "") or kind),
        target_metric=_infer_target_metric(kind=kind, question=question, amount_column=columns.amount_column, draft=draft),
        target_dimension=_infer_target_dimension(kind=kind, columns=columns, draft=draft),
        comparison_type=_infer_comparison_type(kind),
        time_scope=_infer_time_scope(question=question, followup_context=followup_context, draft=draft),
        answer_expectation=_infer_answer_expectation(kind=kind, mode=mode, draft=draft, clarification=clarification),
        clarification=clarification,
    )
