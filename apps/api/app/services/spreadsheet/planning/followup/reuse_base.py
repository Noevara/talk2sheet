from __future__ import annotations

import re
from typing import Any, Callable

from ...pipeline.column_profile import normalize_text
from ...execution.executor import apply_selection, apply_transform
from .planner_followup_context import (
    _followup_last_intent,
    _followup_last_turn,
    _interpreted_value,
)
from ...core.schema import ChartSpec, Filter, SelectionPlan, TransformPlan
from ..planner_text_utils import _contains_any
from ..planner_time import (
    _extract_date_literal,
    _extract_month_literal,
)


def _match_question_value(df: Any, column: str, question: str) -> str | None:
    if column not in getattr(df, "columns", []):
        return None
    normalized_question = normalize_text(question)
    candidates: list[str] = []
    seen: set[str] = set()
    for value in list(df[column].dropna().astype(str).head(300)):
        clean = str(value).strip()
        if not clean or clean in seen:
            continue
        seen.add(clean)
        normalized_value = normalize_text(clean)
        if not normalized_value:
            continue
        if normalized_value in normalized_question:
            candidates.append(clean)
    if not candidates:
        return None
    candidates.sort(key=lambda item: (len(item), item), reverse=True)
    return candidates[0]


def _load_previous_structured_turn(
    followup_context: dict[str, Any] | None,
) -> tuple[str, SelectionPlan, TransformPlan, ChartSpec | None] | None:
    if not isinstance(followup_context, dict):
        return None
    last_turn = _followup_last_turn(followup_context)
    if not isinstance(last_turn, dict):
        return None

    selection_payload = last_turn.get("selection_plan")
    transform_payload = last_turn.get("transform_plan")
    if not isinstance(selection_payload, dict) or not isinstance(transform_payload, dict):
        return None

    try:
        selection_plan = SelectionPlan.model_validate(selection_payload)
        transform_plan = TransformPlan.model_validate(transform_payload)
    except Exception:
        return None

    chart_spec: ChartSpec | None = None
    chart_payload = last_turn.get("chart_spec")
    if isinstance(chart_payload, dict):
        try:
            chart_spec = ChartSpec.model_validate(chart_payload)
        except Exception:
            chart_spec = None

    return _followup_last_intent(followup_context), selection_plan, transform_plan, chart_spec


def _metric_alias_from_plan(transform_plan: TransformPlan) -> str | None:
    if transform_plan.metrics:
        metric_alias = str(transform_plan.metrics[0].as_name or transform_plan.metrics[0].col or "")
        return metric_alias or None
    return None


def _metric_input_columns(transform_plan: TransformPlan) -> list[str]:
    out: list[str] = []
    for metric in transform_plan.metrics or []:
        if metric.col:
            out.append(str(metric.col))
    return list(dict.fromkeys(out))


def _previous_ranked_row(
    df: Any,
    followup_context: dict[str, Any] | None,
    *,
    rank_position: int = 1,
) -> tuple[str, Any, str | None, Any] | None:
    previous = _load_previous_structured_turn(followup_context)
    if previous is None:
        return None
    intent, selection_plan, transform_plan, _chart_spec = previous
    if intent != "ranking":
        return None

    try:
        selected_df, _ = apply_selection(df, selection_plan)
        result_df, _ = apply_transform(selected_df, transform_plan)
    except Exception:
        return None

    if int(rank_position) < 0:
        row_index = len(result_df.index) + int(rank_position)
    else:
        row_index = int(rank_position) - 1
    row_index = max(0, row_index)
    if len(result_df.index) <= row_index or len(result_df.columns) < 1:
        return None

    groupby = list(transform_plan.groupby or [])
    dimension_column = groupby[0] if groupby else str(result_df.columns[0])
    if dimension_column not in result_df.columns:
        dimension_column = str(result_df.columns[0])

    metric_alias = _metric_alias_from_plan(transform_plan)
    if metric_alias and metric_alias not in result_df.columns:
        metric_alias = None

    target_row = result_df.iloc[row_index]
    metric_value = target_row[metric_alias] if metric_alias and metric_alias in result_df.columns else None
    return dimension_column, target_row[dimension_column], metric_alias, metric_value


def _previous_ranking_target(
    df: Any,
    followup_context: dict[str, Any] | None,
    *,
    rank_position: int = 1,
) -> tuple[str, Any, str | None] | None:
    target = _previous_ranked_row(df, followup_context, rank_position=rank_position)
    if target is None:
        return None
    dimension_column, target_value, metric_alias, _metric_value = target
    return dimension_column, target_value, metric_alias


def _previous_ranking_target_from_question(
    df: Any,
    chat_text: str,
    followup_context: dict[str, Any] | None,
    *,
    rank_position_from_text: Callable[[str], int | None],
) -> tuple[str, Any, str | None, int] | None:
    previous = _load_previous_structured_turn(followup_context)
    if previous is None:
        return None
    intent, selection_plan, transform_plan, _chart_spec = previous
    if intent != "ranking":
        return None
    try:
        selected_df, _ = apply_selection(df, selection_plan)
        result_df, _ = apply_transform(selected_df, transform_plan)
    except Exception:
        return None
    if len(result_df.index) == 0 or len(result_df.columns) < 1:
        return None

    groupby = list(transform_plan.groupby or [])
    dimension_column = groupby[0] if groupby else str(result_df.columns[0])
    if dimension_column not in result_df.columns:
        dimension_column = str(result_df.columns[0])
    metric_alias = _metric_alias_from_plan(transform_plan)
    if metric_alias and metric_alias not in result_df.columns:
        metric_alias = None

    explicit_target = str(_interpreted_value(followup_context, "target_label", "") or "").strip()
    if explicit_target:
        for idx, value in enumerate(result_df[dimension_column].tolist(), start=1):
            if str(value) == explicit_target:
                return dimension_column, explicit_target, metric_alias, idx

    explicit_target = _match_question_value(result_df, dimension_column, chat_text)
    if explicit_target is not None:
        for idx, value in enumerate(result_df[dimension_column].tolist(), start=1):
            if str(value) == str(explicit_target):
                return dimension_column, explicit_target, metric_alias, idx

    interpreted_rank = _interpreted_value(followup_context, "target_rank")
    if interpreted_rank is not None:
        try:
            rank_position = int(interpreted_rank)
        except Exception:
            rank_position = None
    else:
        rank_position = rank_position_from_text(chat_text)
    if rank_position is not None:
        target = _previous_ranking_target(df, followup_context, rank_position=rank_position)
        if target is not None:
            return target[0], target[1], target[2], rank_position

    references_previous = _contains_any(
        chat_text,
        (
            "上面",
            "这个",
            "前一个",
            "刚才",
            "第一个",
            "第一名",
            "排第一个",
            "top1",
            "top 1",
            "leader",
        ),
    )
    if references_previous:
        target = _previous_ranking_target(df, followup_context, rank_position=1)
        if target is not None:
            return target[0], target[1], target[2], 1
    return None


def _previous_filtered_target(followup_context: dict[str, Any] | None) -> tuple[str, Any] | None:
    previous = _load_previous_structured_turn(followup_context)
    if previous is None:
        return None
    intent, selection_plan, _transform_plan, _chart_spec = previous
    if intent != "explain_ranked_item":
        return None
    filters = list(selection_plan.filters or [])
    if not filters:
        return None
    first_filter = filters[0]
    if first_filter.op != "=":
        return None
    return str(first_filter.col), first_filter.value


def _make_chart_spec_for_followup(
    *,
    intent: str,
    mode: str,
    chat_text: str,
    chart_spec: ChartSpec | None,
    transform_plan: TransformPlan,
) -> ChartSpec | None:
    if mode != "chart":
        return None
    x = transform_plan.groupby[0] if transform_plan.groupby else (chart_spec.x if chart_spec is not None else "label")
    y = _metric_alias_from_plan(transform_plan) or (chart_spec.y if chart_spec is not None else "value")
    if chart_spec is not None:
        return chart_spec.model_copy(
            update={
                "title": chat_text.strip() or chart_spec.title,
                "x": x,
                "y": y,
                "top_k": transform_plan.top_k,
                "type": "pie" if intent == "share" else chart_spec.type,
            }
        )
    return ChartSpec(
        type="pie" if intent == "share" else "bar",
        title=chat_text.strip() or ("Share" if intent == "share" else "Chart"),
        x=x,
        y=y or "value",
        top_k=transform_plan.top_k,
    )


def _dedupe_columns(columns: list[str]) -> list[str]:
    return list(dict.fromkeys(str(column) for column in columns if str(column or "").strip()))


def _replace_filter(selection_plan: SelectionPlan, *, column: str, op: str, value: Any) -> SelectionPlan:
    filters = [flt for flt in selection_plan.filters if str(flt.col) != str(column)]
    filters.append(Filter(col=column, op=op, value=value))
    return selection_plan.model_copy(update={"filters": filters})


def _remove_filter(selection_plan: SelectionPlan, *, column: str) -> SelectionPlan:
    filters = [flt for flt in selection_plan.filters if str(flt.col) != str(column)]
    return selection_plan.model_copy(update={"filters": filters})


def _quarter_literal(value: str) -> tuple[int, int] | None:
    match = re.search(r"(\d{4})[-/年]?Q([1-4])", str(value or ""), flags=re.I)
    if match:
        return int(match.group(1)), int(match.group(2))
    match = re.search(r"(\d{4})年?第?([1-4])季度", str(value or ""), flags=re.I)
    if match:
        return int(match.group(1)), int(match.group(2))
    return None


def _previous_period_literal(value: str, *, grain: str, basis: str) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    if grain == "day":
        matched = _extract_date_literal(text)
        if not matched:
            return None
        year, month, day = [int(item) for item in matched.split("-")]
        try:
            from datetime import date, timedelta

            current = date(year, month, day)
            previous = date(year - 1, month, day) if basis == "year_over_year" else current - timedelta(days=1)
            return previous.isoformat()
        except Exception:
            return None
    if grain == "week":
        match = re.search(r"(\d{4})-W(\d{2})", text, flags=re.I)
        if not match:
            return None
        year = int(match.group(1))
        week = int(match.group(2))
        if basis == "year_over_year":
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
        if basis == "year_over_year":
            return f"{year - 1:04d}-{month:02d}"
        month -= 1
        if month >= 1:
            return f"{year:04d}-{month:02d}"
        return f"{year - 1:04d}-12"
    if grain == "quarter":
        matched = _quarter_literal(text)
        if not matched:
            return None
        year, quarter = matched
        if basis == "year_over_year":
            return f"{year - 1:04d}-Q{quarter}"
        quarter -= 1
        if quarter >= 1:
            return f"{year:04d}-Q{quarter}"
        return f"{year - 1:04d}-Q4"
    return None


def _time_filter_from_selection(selection_plan: SelectionPlan, *, date_column: str | None) -> tuple[str, str, str] | None:
    if not date_column:
        return None
    for flt in selection_plan.filters or []:
        if str(flt.col) != str(date_column):
            continue
        value = str(flt.value or "").strip()
        if not value:
            continue
        if flt.op == "=" and _extract_date_literal(value):
            return "day", "=", _extract_date_literal(value) or value
        if flt.op == "contains":
            month = _extract_month_literal(value)
            if month:
                return "month", "contains", month
            quarter = _quarter_literal(value)
            if quarter:
                return "quarter", "contains", f"{quarter[0]:04d}-Q{quarter[1]}"
    return None


def _trend_period_pair_from_previous_result(
    df: Any,
    followup_context: dict[str, Any] | None,
    *,
    basis: str,
) -> tuple[str, str, str] | None:
    previous = _load_previous_structured_turn(followup_context)
    if previous is None:
        return None
    intent, selection_plan, transform_plan, _chart_spec = previous
    if intent != "trend":
        return None
    if not transform_plan.derived_columns:
        return None
    bucket = transform_plan.derived_columns[0]
    if bucket.kind != "date_bucket" or not bucket.grain:
        return None
    try:
        selected_df, _ = apply_selection(df, selection_plan)
        result_df, _ = apply_transform(selected_df, transform_plan)
    except Exception:
        return None
    if len(result_df.index) == 0 or not transform_plan.groupby:
        return None
    dimension_column = transform_plan.groupby[0]
    if dimension_column not in result_df.columns:
        return None
    current_value = str(result_df.iloc[-1][dimension_column] or "").strip()
    previous_value = _previous_period_literal(current_value, grain=str(bucket.grain), basis=basis)
    if not current_value or not previous_value:
        return None
    return str(bucket.grain), current_value, previous_value
