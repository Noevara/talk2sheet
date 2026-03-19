from __future__ import annotations

from typing import Any, Callable

from .reuse_base import (
    _dedupe_columns,
    _load_previous_structured_turn,
    _make_chart_spec_for_followup,
    _metric_input_columns,
    _previous_ranked_row,
    _replace_filter,
)
from ...core.schema import ChartSpec, DerivedColumn, Metric, SelectionPlan, Sort, TransformPlan
from ..planner_intent_signals import (
    _day_count_question,
    _forecast_question,
)
from ..planner_time import _extract_time_grain, _time_bucket_alias


def _reuse_previous_plan_with_top_k(
    chat_text: str,
    *,
    mode: str,
    followup_context: dict[str, Any] | None,
    top_k_followup: Callable[[str, dict[str, Any] | None], int | None],
    plan_draft_factory: Callable[..., Any],
) -> Any | None:
    top_k = top_k_followup(chat_text, followup_context)
    if top_k is None:
        return None
    previous = _load_previous_structured_turn(followup_context)
    if previous is None:
        return None

    intent, selection_plan, transform_plan, chart_spec = previous

    if intent == "detail_rows":
        if selection_plan.sort is None:
            return None
        selection_plan = selection_plan.model_copy(update={"limit": top_k})
        return plan_draft_factory(
            mode="text",
            intent="detail_rows",
            selection_plan=selection_plan,
            transform_plan=TransformPlan(return_rows=True),
            planner_meta={"top_k": top_k, "followup_reused_previous_plan": True},
        )

    transform_plan = transform_plan.model_copy(update={"top_k": top_k})
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
        planner_meta={"top_k": top_k, "followup_reused_previous_plan": True},
    )


def _reuse_previous_plan_with_mode_switch(
    chat_text: str,
    *,
    mode: str,
    followup_context: dict[str, Any] | None,
    mode_switch_followup: Callable[[str], bool] | Callable[..., bool],
    plan_draft_factory: Callable[..., Any],
) -> Any | None:
    if not mode_switch_followup(chat_text, mode=mode, followup_context=followup_context):
        return None
    previous = _load_previous_structured_turn(followup_context)
    if previous is None:
        return None
    intent, selection_plan, transform_plan, chart_spec = previous
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
        planner_meta={"followup_reused_previous_plan": True, "followup_mode_switch": True},
    )


def _reuse_previous_plan_for_rank_position(
    df: Any,
    chat_text: str,
    *,
    followup_context: dict[str, Any] | None,
    rank_lookup_followup: Callable[[str, dict[str, Any] | None], int | None],
    plan_draft_factory: Callable[..., Any],
) -> Any | None:
    rank_position = rank_lookup_followup(chat_text, followup_context)
    if rank_position is None:
        return None
    previous = _load_previous_structured_turn(followup_context)
    if previous is None:
        return None
    intent, selection_plan, transform_plan, _chart_spec = previous
    if intent != "ranking":
        return None
    ranked_row = _previous_ranked_row(df, followup_context, rank_position=rank_position)
    if ranked_row is None:
        return None

    dimension_column, target_value, _metric_alias, metric_value = ranked_row
    selection_columns = _dedupe_columns([dimension_column, *_metric_input_columns(transform_plan)])
    filtered_plan = _replace_filter(
        selection_plan.model_copy(update={"columns": selection_columns, "limit": None, "sort": None, "distinct_by": None}),
        column=dimension_column,
        op="=",
        value=target_value,
    )
    focused_transform = transform_plan.model_copy(update={"top_k": 1})
    return plan_draft_factory(
        mode="text",
        intent="ranked_item_lookup",
        selection_plan=filtered_plan,
        transform_plan=focused_transform,
        planner_meta={
            "rank_position": rank_position,
            "ranked_item_dimension_column": dimension_column,
            "ranked_item_value": target_value,
            "ranked_item_metric_value": metric_value,
            "followup_reused_previous_plan": True,
        },
    )


def _reuse_previous_plan_with_dimension(
    chat_text: str,
    *,
    mode: str,
    new_dimension: str | None,
    followup_context: dict[str, Any] | None,
    dimension_switch_followup: Callable[..., bool],
    plan_draft_factory: Callable[..., Any],
) -> Any | None:
    if not dimension_switch_followup(chat_text, new_dimension=new_dimension, followup_context=followup_context):
        return None
    previous = _load_previous_structured_turn(followup_context)
    if previous is None or new_dimension is None:
        return None
    intent, selection_plan, transform_plan, chart_spec = previous
    current_dimension = transform_plan.groupby[0] if transform_plan.groupby else None
    if current_dimension == new_dimension:
        return None

    selection_columns = _dedupe_columns([new_dimension, *_metric_input_columns(transform_plan)])
    selection_plan = selection_plan.model_copy(update={"columns": selection_columns, "limit": None, "sort": None, "distinct_by": None})
    transform_plan = transform_plan.model_copy(
        update={
            "groupby": [new_dimension],
            "return_rows": False,
            "order_by": transform_plan.order_by or Sort(col="value", dir="desc"),
        }
    )
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
            "switch_dimension_to": new_dimension,
            "followup_reused_previous_plan": True,
        },
    )


def _reuse_previous_plan_as_share(
    chat_text: str,
    *,
    mode: str,
    followup_context: dict[str, Any] | None,
    share_switch_followup: Callable[[str, dict[str, Any] | None], bool],
    plan_draft_factory: Callable[..., Any],
) -> Any | None:
    if not share_switch_followup(chat_text, followup_context):
        return None
    previous = _load_previous_structured_turn(followup_context)
    if previous is None:
        return None
    intent, selection_plan, transform_plan, chart_spec = previous
    if intent not in {"ranking", "share"} or not transform_plan.groupby:
        return None
    return plan_draft_factory(
        mode=mode,
        intent="share",
        selection_plan=selection_plan,
        transform_plan=transform_plan,
        chart_spec=_make_chart_spec_for_followup(
            intent="share",
            mode=mode,
            chat_text=chat_text,
            chart_spec=chart_spec,
            transform_plan=transform_plan,
        ),
        planner_meta={"followup_reused_previous_plan": True, "share_dimension": transform_plan.groupby[0]},
    )


def _reuse_previous_plan_with_time_filter(
    chat_text: str,
    *,
    mode: str,
    date_column: str | None,
    followup_context: dict[str, Any] | None,
    time_filter_followup: Callable[[str, dict[str, Any] | None], tuple[str, str] | None],
    plan_draft_factory: Callable[..., Any],
) -> Any | None:
    time_filter = time_filter_followup(chat_text, followup_context)
    if time_filter is None or not date_column:
        return None
    previous = _load_previous_structured_turn(followup_context)
    if previous is None:
        return None
    intent, selection_plan, transform_plan, chart_spec = previous
    if intent not in {"ranking", "share", "trend", "detail_rows", "total_amount", "average_amount", "count_distinct", "row_count"}:
        return None
    if _day_count_question(chat_text) or _forecast_question(chat_text):
        return None
    selection_plan = _replace_filter(
        selection_plan.model_copy(update={"limit": selection_plan.limit if intent == "detail_rows" else None}),
        column=date_column,
        op=time_filter[0],
        value=time_filter[1],
    )
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
            "date_filter_column": date_column,
            "date_filter_value": time_filter[1],
            "date_filter_op": time_filter[0],
            "followup_reused_previous_plan": True,
        },
    )


def _reuse_previous_plan_as_trend(
    chat_text: str,
    *,
    mode: str,
    date_column: str | None,
    followup_context: dict[str, Any] | None,
    trend_switch_followup: Callable[[str, dict[str, Any] | None], bool],
    plan_draft_factory: Callable[..., Any],
) -> Any | None:
    if not date_column or not trend_switch_followup(chat_text, followup_context):
        return None
    previous = _load_previous_structured_turn(followup_context)
    if previous is None:
        return None
    _intent, selection_plan, transform_plan, _chart_spec = previous
    metric_columns = _metric_input_columns(transform_plan)
    amount_column = metric_columns[0] if metric_columns else None
    if not amount_column:
        return None
    from .planner_followup_context import _interpreted_value

    bucket_grain = str(_interpreted_value(followup_context, "time_grain", "") or "").strip() or _extract_time_grain(chat_text, default="month")
    bucket_name = _time_bucket_alias(bucket_grain)
    selection_plan = selection_plan.model_copy(update={"columns": _dedupe_columns([date_column, amount_column])})
    trend_transform = TransformPlan(
        derived_columns=[DerivedColumn(as_name=bucket_name, kind="date_bucket", source_col=date_column, grain=bucket_grain)],
        groupby=[bucket_name],
        metrics=[Metric(agg="sum", col=amount_column, as_name="value")],
        order_by=Sort(col=bucket_name, dir="asc"),
        top_k=24,
    )
    chart_spec = ChartSpec(type="line", title=chat_text.strip() or "Trend", x=bucket_name, y="value", top_k=24) if mode == "chart" else None
    return plan_draft_factory(
        mode=mode,
        intent="trend",
        selection_plan=selection_plan,
        transform_plan=trend_transform,
        chart_spec=chart_spec,
        planner_meta={
            "followup_reused_previous_plan": True,
            "bucket_name": bucket_name,
            "bucket_grain": bucket_grain,
            "trend_source_metric": amount_column,
        },
    )


def _reuse_previous_plan_as_detail(
    chat_text: str,
    *,
    amount_column: str | None,
    followup_context: dict[str, Any] | None,
    detail_switch_followup: Callable[[str, dict[str, Any] | None], bool],
    plan_draft_factory: Callable[..., Any],
) -> Any | None:
    if not detail_switch_followup(chat_text, followup_context):
        return None
    previous = _load_previous_structured_turn(followup_context)
    if previous is None:
        return None
    _intent, selection_plan, transform_plan, _chart_spec = previous
    metric_columns = _metric_input_columns(transform_plan)
    selection_columns = selection_plan.columns or _dedupe_columns([*(transform_plan.groupby or []), *metric_columns])
    sort_column = amount_column or (metric_columns[0] if metric_columns else None)
    detail_selection = selection_plan.model_copy(
        update={
            "columns": selection_columns,
            "sort": Sort(col=sort_column, dir="desc") if sort_column else selection_plan.sort,
            "limit": 10,
            "distinct_by": None,
        }
    )
    return plan_draft_factory(
        mode="text",
        intent="detail_rows",
        selection_plan=detail_selection,
        transform_plan=TransformPlan(return_rows=True),
        planner_meta={"followup_reused_previous_plan": True, "detail_sort_column": sort_column or ""},
    )
