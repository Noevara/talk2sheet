from __future__ import annotations

from typing import Any, Callable

from .planner_followup_context import _compare_basis, _contextual_followup
from .reuse_base import (
    _dedupe_columns,
    _load_previous_structured_turn,
    _metric_input_columns,
    _previous_filtered_target,
    _previous_period_literal,
    _previous_ranking_target_from_question,
    _remove_filter,
    _replace_filter,
    _time_filter_from_selection,
    _trend_period_pair_from_previous_result,
)
from ...core.schema import DerivedColumn, Filter, FormulaMetric, Metric, PivotSpec, Sort, TransformPlan
from ..planner_intent_signals import _compare_question
from ..planner_text_utils import _extract_top_k
from ..planner_time import _time_bucket_alias


def _comparison_period_context(
    df: Any,
    *,
    chat_text: str,
    date_column: str | None,
    followup_context: dict[str, Any] | None,
) -> tuple[str, str, str, str] | None:
    previous = _load_previous_structured_turn(followup_context)
    if previous is None:
        return None
    _intent, selection_plan, _transform_plan, _chart_spec = previous
    basis = _compare_basis(chat_text, followup_context)
    from_selection = _time_filter_from_selection(selection_plan, date_column=date_column)
    if from_selection is not None:
        grain, op, current_value = from_selection
        previous_value = _previous_period_literal(current_value, grain=grain, basis=basis)
        if previous_value:
            return grain, op, current_value, previous_value
    from_trend = _trend_period_pair_from_previous_result(df, followup_context, basis=basis)
    if from_trend is not None:
        grain, current_value, previous_value = from_trend
        op = "=" if grain == "day" else "contains"
        return grain, op, current_value, previous_value
    return None


def _question_value_filters(
    df: Any,
    question: str,
    *,
    candidate_columns: list[str],
    followup_context: dict[str, Any] | None = None,
    exclude_values: set[str] | None = None,
) -> list[tuple[str, str]]:
    from .planner_followup_context import _interpreted_value
    from .reuse_base import _match_question_value

    exclude_values = exclude_values or set()
    out: list[tuple[str, str]] = []
    seen_columns: set[str] = set()
    interpreted_filters = _interpreted_value(followup_context, "value_filters", [])
    if isinstance(interpreted_filters, list):
        for item in interpreted_filters:
            if not isinstance(item, dict):
                continue
            value = str(item.get("value") or "").strip()
            column_hint = str(item.get("column_hint") or "").strip()
            if not value or value in exclude_values:
                continue
            if column_hint and column_hint in candidate_columns:
                out.append((column_hint, value))
    for column in candidate_columns:
        if not column or column in seen_columns:
            continue
        seen_columns.add(column)
        matched = _match_question_value(df, column, question)
        if matched is None or matched in exclude_values:
            continue
        out.append((column, matched))
    return out


def _reuse_previous_plan_with_value_filters(
    df: Any,
    chat_text: str,
    *,
    mode: str,
    candidate_columns: list[str],
    date_column: str | None,
    followup_context: dict[str, Any] | None,
    time_filter_followup: Callable[[str, dict[str, Any] | None], tuple[str, str] | None],
    plan_draft_factory: Callable[..., Any],
) -> Any | None:
    from .reuse_base import _make_chart_spec_for_followup

    if not _contextual_followup(chat_text, followup_context):
        return None
    previous = _load_previous_structured_turn(followup_context)
    if previous is None:
        return None
    intent, selection_plan, transform_plan, chart_spec = previous
    if intent not in {"ranking", "share", "trend", "detail_rows"}:
        return None

    time_filter = time_filter_followup(chat_text, followup_context)
    filters = _question_value_filters(
        df,
        chat_text,
        candidate_columns=[column for column in candidate_columns if column and column != date_column],
        followup_context=followup_context,
    )
    if time_filter is None and not filters:
        return None

    for column, value in filters:
        selection_plan = _replace_filter(selection_plan, column=column, op="=", value=value)
    if time_filter is not None and date_column:
        selection_plan = _replace_filter(selection_plan, column=date_column, op=time_filter[0], value=time_filter[1])

    return plan_draft_factory(
        mode=mode,
        intent=intent,
        selection_plan=selection_plan,
        transform_plan=transform_plan,
        chart_spec=_make_chart_spec_for_followup(
            intent=intent,
            mode=mode,
            chat_text=chat_text,
            chart_spec=chart_spec,
            transform_plan=transform_plan,
        ),
        planner_meta={
            "followup_reused_previous_plan": True,
            "value_filters": [{"column": column, "value": value} for column, value in filters],
            "date_filter_column": date_column,
            "date_filter_value": time_filter[1] if time_filter is not None else "",
            "date_filter_op": time_filter[0] if time_filter is not None else "",
        },
    )


def _reuse_previous_plan_as_period_compare(
    df: Any,
    chat_text: str,
    *,
    date_column: str | None,
    amount_column: str | None,
    followup_context: dict[str, Any] | None,
    plan_draft_factory: Callable[..., Any],
) -> Any | None:
    if not date_column or not _compare_question(chat_text) or not _contextual_followup(chat_text, followup_context):
        return None
    previous = _load_previous_structured_turn(followup_context)
    if previous is None:
        return None
    _intent, selection_plan, transform_plan, _chart_spec = previous
    metric_columns = _metric_input_columns(transform_plan)
    compare_metric_column = metric_columns[0] if metric_columns else amount_column
    if not compare_metric_column:
        return None
    period_context = _comparison_period_context(
        df,
        chat_text=chat_text,
        date_column=date_column,
        followup_context=followup_context,
    )
    if period_context is None:
        return None
    grain, _op, current_period, previous_period = period_context
    basis = _compare_basis(chat_text, followup_context)

    selection_plan = _remove_filter(selection_plan, column=date_column)
    selection_plan = selection_plan.model_copy(
        update={
            "columns": _dedupe_columns([date_column, compare_metric_column]),
            "sort": None,
            "limit": None,
            "distinct_by": None,
        }
    )
    bucket_name = _time_bucket_alias(grain)
    transform_plan = TransformPlan(
        derived_columns=[DerivedColumn(as_name=bucket_name, kind="date_bucket", source_col=date_column, grain=grain)],
        groupby=[bucket_name],
        metrics=[Metric(agg="sum", col=compare_metric_column, as_name="value")],
        having=[Filter(col=bucket_name, op="in", value=[previous_period, current_period])],
        pivot=PivotSpec(index=[], columns=bucket_name, values="value", fill_value=0),
        post_pivot_formula_metrics=[
            FormulaMetric(as_name="change_value", op="sub", left=current_period, right=previous_period),
            FormulaMetric(as_name="change_pct", op="div", left="change_value", right=previous_period),
        ],
    )
    return plan_draft_factory(
        mode="text",
        intent="period_compare",
        selection_plan=selection_plan,
        transform_plan=transform_plan,
        planner_meta={
            "followup_reused_previous_plan": True,
            "compare_basis": basis,
            "compare_grain": grain,
            "current_period": current_period,
            "previous_period": previous_period,
            "compare_metric_column": compare_metric_column,
        },
    )


def _reuse_previous_plan_as_explain_breakdown(
    df: Any,
    chat_text: str,
    *,
    amount_column: str | None,
    new_dimension: str | None,
    followup_context: dict[str, Any] | None,
    rank_position_from_text: Callable[[str], int | None],
    breakdown_followup: Callable[[str, dict[str, Any] | None], bool],
    plan_draft_factory: Callable[..., Any],
) -> Any | None:
    if not new_dimension or not breakdown_followup(chat_text, followup_context):
        return None
    previous = _load_previous_structured_turn(followup_context)
    if previous is None:
        return None
    intent, selection_plan, transform_plan, _chart_spec = previous
    metric_columns = _metric_input_columns(transform_plan)
    breakdown_metric_column = metric_columns[0] if metric_columns else amount_column
    if not breakdown_metric_column:
        return None

    target_dimension: str | None = None
    target_value: Any = None
    if intent == "explain_ranked_item":
        target = _previous_filtered_target(followup_context)
        if target is not None:
            target_dimension, target_value = target
    if target_dimension is None:
        target = _previous_ranking_target_from_question(
            df,
            chat_text,
            followup_context,
            rank_position_from_text=rank_position_from_text,
        )
        if target is not None:
            target_dimension, target_value = target[0], target[1]
    if target_dimension is None or target_value in {None, ""}:
        return None
    if str(target_dimension) == str(new_dimension):
        return None

    selection_plan = selection_plan.model_copy(
        update={
            "columns": _dedupe_columns([new_dimension, breakdown_metric_column]),
            "sort": None,
            "limit": None,
            "distinct_by": None,
        }
    )
    selection_plan = _replace_filter(selection_plan, column=target_dimension, op="=", value=target_value)
    transform_plan = TransformPlan(
        groupby=[new_dimension],
        metrics=[Metric(agg="sum", col=breakdown_metric_column, as_name="value")],
        order_by=Sort(col="value", dir="desc"),
        top_k=_extract_top_k(chat_text, default=5),
    )
    return plan_draft_factory(
        mode="text",
        intent="explain_breakdown",
        selection_plan=selection_plan,
        transform_plan=transform_plan,
        planner_meta={
            "followup_reused_previous_plan": True,
            "breakdown_dimension": new_dimension,
            "breakdown_target_dimension": target_dimension,
            "breakdown_target_value": target_value,
            "breakdown_metric_column": breakdown_metric_column,
        },
    )


def _explanation_columns(
    profiles: dict[str, dict[str, Any]],
    *,
    dimension_column: str,
    amount_column: str | None,
    date_column: str | None,
) -> list[str]:
    columns: list[str] = [dimension_column]
    preferred_tokens = (
        "产品名称",
        "商品名称",
        "计费项名称",
        "服务名称",
        "地域",
        "地区",
        "区域",
        "客户",
        "账号",
        "资源名称",
        "实例ID",
        "资源ID",
        "消费时间",
        "账单日期",
        "账单月份",
    )
    for token in preferred_tokens:
        for column in profiles:
            if column in columns:
                continue
            if token in column:
                columns.append(column)
                break
    if date_column and date_column not in columns:
        columns.append(date_column)
    if amount_column and amount_column not in columns:
        columns.append(amount_column)
    return columns[:8]
