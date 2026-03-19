from __future__ import annotations

from typing import Any, Callable

from ..execution.fast_paths import try_chart_fast_path, try_text_fast_path
from .followup.planner_followup_context import _detail_to_ranking_followup
from .planner_intent_signals import (
    _amount_question,
    _average_amount_question,
    _count_question,
    _day_count_question,
    _detail_question,
    _distinct_question,
    _extract_chart_type,
    _forecast_question,
    _item_ranking_question,
    _multi_period_amount_question,
    _ranking_question,
    _row_count_question,
    _share_question,
    _single_transaction_question,
    _total_amount_question,
    _trend_question,
    _weekday_weekend_question,
)
from .planner_text_utils import _extract_top_k
from .planner_time import (
    _build_month_range_filters,
    _extract_time_grain,
    _resolve_forecast_targets,
    _resolve_requested_single_month_bucket,
    _time_bucket_alias,
)
from ..core.schema import ChartSpec, DerivedColumn, Filter, Metric, SelectionPlan, Sort, TransformPlan
from .planner_types import HeuristicActionRuntimeContext


def _find_column_by_hint(profiles: dict[str, dict[str, Any]], hint: str) -> str | None:
    for column, profile in profiles.items():
        if hint in (profile.get("semantic_hints") or []):
            return column
    return None


def build_forecast_plan(
    df: Any,
    *,
    chat_text: str,
    date_column: str | None,
    amount_column: str | None,
    planner_meta: dict[str, Any],
    plan_draft_factory: Callable[..., Any],
) -> Any | None:
    if not _forecast_question(chat_text):
        return None
    forecast_grain = _extract_time_grain(chat_text, default="month")
    forecast_targets = _resolve_forecast_targets(df, date_column=date_column, chat_text=chat_text, grain=forecast_grain)
    if amount_column and date_column and forecast_targets is not None:
        target_periods, horizon = forecast_targets
        target_period = target_periods[-1]
        bucket_name = _time_bucket_alias(forecast_grain)
        return plan_draft_factory(
            mode="text",
            intent="forecast_timeseries",
            selection_plan=SelectionPlan(columns=[date_column, amount_column]),
            transform_plan=TransformPlan(
                derived_columns=[DerivedColumn(as_name=bucket_name, kind="date_bucket", source_col=date_column, grain=forecast_grain)],
                groupby=[bucket_name],
                metrics=[Metric(agg="sum", col=amount_column, as_name="value")],
                order_by=Sort(col=bucket_name, dir="asc"),
            ),
            planner_meta={
                **planner_meta,
                "forecast_grain": forecast_grain,
                "forecast_target_period": target_period,
                "forecast_target_periods": target_periods,
                "forecast_target_start_period": target_periods[0],
                "forecast_target_end_period": target_periods[-1],
                "forecast_target_count": len(target_periods),
                "forecast_horizon": horizon,
                "bucket_name": bucket_name,
            },
        )
    return plan_draft_factory(
        mode="text",
        intent="unsupported",
        selection_plan=SelectionPlan(),
        transform_plan=TransformPlan(),
        planner_meta={**planner_meta, "fallback": True, "unsupported_reason": "forecast_not_supported"},
    )


def try_build_heuristic_action(
    df: Any,
    *,
    chat_text: str,
    runtime_context: HeuristicActionRuntimeContext,
    plan_draft_factory: Callable[..., Any],
) -> Any | None:
    effective_chat_text = runtime_context.effective_chat_text
    mode = runtime_context.mode
    followup_context = runtime_context.followup_context
    preserve_previous_analysis = runtime_context.preserve_previous_analysis
    profiles = runtime_context.profiles
    planner_meta = runtime_context.planner_meta
    amount_column = runtime_context.amount_column
    date_column = runtime_context.date_column
    category_column = runtime_context.category_column
    single_transaction_column = runtime_context.single_transaction_column
    item_preferred_column = runtime_context.item_preferred_column
    item_column = runtime_context.item_column
    question_dimension_column = runtime_context.question_dimension_column

    if _row_count_question(effective_chat_text) and not preserve_previous_analysis:
        return plan_draft_factory(
            mode="text",
            intent="row_count",
            selection_plan=SelectionPlan(),
            transform_plan=TransformPlan(metrics=[Metric(agg="count_rows", as_name="row_count")]),
            planner_meta=planner_meta,
        )

    if _distinct_question(effective_chat_text) and not preserve_previous_analysis:
        distinct_column = category_column or _find_column_by_hint(profiles, "id") or amount_column or date_column
        if distinct_column:
            return plan_draft_factory(
                mode="text",
                intent="count_distinct",
                selection_plan=SelectionPlan(columns=[distinct_column]),
                transform_plan=TransformPlan(metrics=[Metric(agg="count_distinct", col=distinct_column, as_name="distinct_count")]),
                planner_meta={**planner_meta, "distinct_column": distinct_column},
            )

    requested_single_month = _resolve_requested_single_month_bucket(df, date_column=date_column, chat_text=chat_text)
    if requested_single_month and date_column and _day_count_question(chat_text) and not preserve_previous_analysis:
        bucket_name = _time_bucket_alias("day")
        return plan_draft_factory(
            mode="text",
            intent="active_day_count",
            selection_plan=SelectionPlan(
                columns=[date_column],
                filters=_build_month_range_filters(date_column, requested_single_month),
            ),
            transform_plan=TransformPlan(
                derived_columns=[DerivedColumn(as_name=bucket_name, kind="date_bucket", source_col=date_column, grain="day")],
                metrics=[Metric(agg="count_distinct", col=bucket_name, as_name="active_day_count")],
            ),
            planner_meta={**planner_meta, "requested_period": requested_single_month, "bucket_name": bucket_name},
        )

    if requested_single_month and amount_column and date_column and _total_amount_question(chat_text) and not preserve_previous_analysis:
        return plan_draft_factory(
            mode="text",
            intent="total_amount",
            selection_plan=SelectionPlan(
                columns=[date_column, amount_column],
                filters=_build_month_range_filters(date_column, requested_single_month),
            ),
            transform_plan=TransformPlan(metrics=[Metric(agg="sum", col=amount_column, as_name="total_amount")]),
            planner_meta={**planner_meta, "requested_period": requested_single_month},
        )

    requested_months = _multi_period_amount_question(df, chat_text=effective_chat_text, date_column=date_column)
    if requested_months and amount_column and date_column and not preserve_previous_analysis:
        bucket_name = _time_bucket_alias("month")
        return plan_draft_factory(
            mode=mode,
            intent="period_breakdown",
            selection_plan=SelectionPlan(columns=[date_column, amount_column]),
            transform_plan=TransformPlan(
                derived_columns=[DerivedColumn(as_name=bucket_name, kind="date_bucket", source_col=date_column, grain="month")],
                groupby=[bucket_name],
                metrics=[Metric(agg="sum", col=amount_column, as_name="value")],
                having=[Filter(col=bucket_name, op="in", value=requested_months)],
                order_by=Sort(col=bucket_name, dir="asc"),
            ),
            chart_spec=ChartSpec(type="bar", title=chat_text.strip() or "Monthly totals", x=bucket_name, y="value", top_k=len(requested_months))
            if mode == "chart"
            else None,
            planner_meta={**planner_meta, "requested_months": requested_months, "bucket_name": bucket_name},
        )

    if _total_amount_question(effective_chat_text) and amount_column and not preserve_previous_analysis:
        return plan_draft_factory(
            mode="text",
            intent="total_amount",
            selection_plan=SelectionPlan(columns=[amount_column]),
            transform_plan=TransformPlan(metrics=[Metric(agg="sum", col=amount_column, as_name="total_amount")]),
            planner_meta=planner_meta,
        )

    if _average_amount_question(effective_chat_text) and amount_column and not preserve_previous_analysis:
        return plan_draft_factory(
            mode="text",
            intent="average_amount",
            selection_plan=SelectionPlan(columns=[amount_column]),
            transform_plan=TransformPlan(metrics=[Metric(agg="avg", col=amount_column, as_name="avg_amount")]),
            planner_meta=planner_meta,
        )

    if _weekday_weekend_question(effective_chat_text) and amount_column and date_column:
        chart_type = _extract_chart_type(effective_chat_text, default="bar")
        return plan_draft_factory(
            mode=mode,
            intent="weekpart_compare",
            selection_plan=SelectionPlan(columns=[date_column, amount_column]),
            transform_plan=TransformPlan(
                derived_columns=[DerivedColumn(as_name="weekpart", kind="date_bucket", source_col=date_column, grain="weekpart")],
                groupby=["weekpart"],
                metrics=[Metric(agg="sum", col=amount_column, as_name="value")],
                order_by=Sort(col="value", dir="desc"),
                top_k=2,
            ),
            chart_spec=ChartSpec(type=chart_type, title=chat_text.strip() or "Weekday vs weekend", x="weekpart", y="value", top_k=2)
            if mode == "chart"
            else None,
            planner_meta=planner_meta,
        )

    if not preserve_previous_analysis and not _single_transaction_question(effective_chat_text):
        fast_path = try_chart_fast_path(df, question=effective_chat_text) if mode == "chart" else try_text_fast_path(df, question=effective_chat_text)
        if isinstance(fast_path, dict):
            return plan_draft_factory(
                mode=str(fast_path.get("mode") or mode),
                intent=str(fast_path.get("intent") or "unsupported"),
                selection_plan=fast_path["selection_plan"],
                transform_plan=fast_path["transform_plan"],
                chart_spec=fast_path.get("chart_spec"),
                planner_meta={**planner_meta, **(fast_path.get("planner_meta") or {})},
            )

    if (_detail_to_ranking_followup(chat_text, followup_context) or _item_ranking_question(effective_chat_text)) and amount_column:
        ranking_column = item_preferred_column or item_column or question_dimension_column or category_column or single_transaction_column
        if ranking_column:
            top_k = _extract_top_k(effective_chat_text, default=5)
            chart_type = _extract_chart_type(effective_chat_text, default="bar")
            ranking_mode = "chart" if mode == "chart" else "text"
            return plan_draft_factory(
                mode=ranking_mode,
                intent="ranking",
                selection_plan=SelectionPlan(columns=[ranking_column, amount_column]),
                transform_plan=TransformPlan(
                    groupby=[ranking_column],
                    metrics=[Metric(agg="sum", col=amount_column, as_name="value")],
                    order_by=Sort(col="value", dir="desc"),
                    top_k=top_k,
                ),
                chart_spec=ChartSpec(
                    type=chart_type,
                    title=chat_text.strip() or "Top items",
                    x=ranking_column,
                    y="value",
                    top_k=top_k,
                )
                if ranking_mode == "chart"
                else None,
                planner_meta={**planner_meta, "top_k": top_k, "ranking_column": ranking_column},
            )

    if _single_transaction_question(effective_chat_text) and amount_column:
        top_k = _extract_top_k(effective_chat_text, default=5)
        if mode == "chart" and single_transaction_column:
            chart_type = _extract_chart_type(effective_chat_text, default="bar")
            return plan_draft_factory(
                mode="chart",
                intent="ranking",
                selection_plan=SelectionPlan(columns=[single_transaction_column, amount_column]),
                transform_plan=TransformPlan(
                    groupby=[single_transaction_column],
                    metrics=[Metric(agg="sum", col=amount_column, as_name="value")],
                    order_by=Sort(col="value", dir="desc"),
                    top_k=top_k,
                ),
                chart_spec=ChartSpec(
                    type=chart_type,
                    title=chat_text.strip() or "Top transactions",
                    x=single_transaction_column,
                    y="value",
                    top_k=top_k,
                ),
                planner_meta={**planner_meta, "top_k": top_k},
            )
        return plan_draft_factory(
            mode="text",
            intent="detail_rows",
            selection_plan=SelectionPlan(sort=Sort(col=amount_column, dir="desc"), limit=top_k),
            transform_plan=TransformPlan(return_rows=True),
            planner_meta={**planner_meta, "top_k": top_k},
        )

    if _trend_question(effective_chat_text) and amount_column and date_column:
        bucket_grain = _extract_time_grain(effective_chat_text, default="month")
        bucket_name = _time_bucket_alias(bucket_grain)
        transform_plan = TransformPlan(
            derived_columns=[DerivedColumn(as_name=bucket_name, kind="date_bucket", source_col=date_column, grain=bucket_grain)],
            groupby=[bucket_name],
            metrics=[Metric(agg="sum", col=amount_column, as_name="value")],
            order_by=Sort(col=bucket_name, dir="asc"),
        )
        return plan_draft_factory(
            mode=mode,
            intent="trend",
            selection_plan=SelectionPlan(columns=[date_column, amount_column]),
            transform_plan=transform_plan,
            chart_spec=ChartSpec(type="line", title=chat_text.strip() or "Trend", x=bucket_name, y="value", top_k=24),
            planner_meta={**planner_meta, "bucket_name": bucket_name, "bucket_grain": bucket_grain},
        )

    if _share_question(effective_chat_text) and category_column:
        share_column = question_dimension_column or category_column
        metric_column = amount_column
        metric = Metric(agg="sum", col=metric_column, as_name="value") if metric_column else Metric(agg="count_rows", as_name="value")
        top_k = _extract_top_k(effective_chat_text, 8)
        chart_type = _extract_chart_type(effective_chat_text, default="pie")
        share_mode = "chart" if mode == "chart" else "text"
        return plan_draft_factory(
            mode=share_mode,
            intent="share",
            selection_plan=SelectionPlan(columns=[share_column] + ([metric_column] if metric_column else [])),
            transform_plan=TransformPlan(groupby=[share_column], metrics=[metric], order_by=Sort(col="value", dir="desc"), top_k=top_k),
            chart_spec=ChartSpec(type=chart_type, title=chat_text.strip() or "Share", x=share_column, y="value", top_k=top_k)
            if share_mode == "chart"
            else None,
            planner_meta={**planner_meta, "share_column": share_column, "top_k": top_k},
        )

    if _detail_question(effective_chat_text) and amount_column:
        top_k = _extract_top_k(effective_chat_text, default=10)
        return plan_draft_factory(
            mode="text",
            intent="detail_rows",
            selection_plan=SelectionPlan(sort=Sort(col=amount_column, dir="desc"), limit=top_k),
            transform_plan=TransformPlan(return_rows=True),
            planner_meta={**planner_meta, "top_k": top_k},
        )

    if _ranking_question(effective_chat_text) and (question_dimension_column or category_column):
        top_k = _extract_top_k(effective_chat_text, default=5)
        ranking_column = question_dimension_column or category_column
        if _count_question(effective_chat_text) and not _amount_question(effective_chat_text):
            metric = Metric(agg="count_rows", as_name="value")
            metric_column = None
        else:
            metric_column = amount_column
            metric = Metric(agg="sum", col=metric_column, as_name="value") if metric_column else Metric(agg="count_rows", as_name="value")
        chart_type = _extract_chart_type(effective_chat_text, default="bar")
        return plan_draft_factory(
            mode=mode,
            intent="ranking",
            selection_plan=SelectionPlan(columns=[ranking_column] + ([metric_column] if metric_column else [])),
            transform_plan=TransformPlan(groupby=[ranking_column], metrics=[metric], order_by=Sort(col="value", dir="desc"), top_k=top_k),
            chart_spec=ChartSpec(type=chart_type, title=chat_text.strip() or "Ranking", x=ranking_column, y="value", top_k=top_k)
            if mode == "chart"
            else None,
            planner_meta={**planner_meta, "ranking_column": ranking_column, "top_k": top_k},
        )

    return None
