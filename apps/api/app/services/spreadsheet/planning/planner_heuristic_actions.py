from __future__ import annotations

import re
from typing import Any, Callable

from ..execution.fast_paths import try_chart_fast_path, try_text_fast_path
from ..pipeline.column_profile import normalize_text
from .followup.planner_followup_context import _detail_to_ranking_followup
from .planner_intent_signals import (
    _amount_question,
    _average_amount_question,
    _compare_question,
    _count_question,
    _delta_compare,
    _day_count_question,
    _detail_question,
    _distinct_question,
    _extract_chart_type,
    _forecast_question,
    _item_ranking_question,
    _multi_period_amount_question,
    _ranking_question,
    _ratio_compare,
    _row_count_question,
    _share_question,
    _single_transaction_question,
    _total_amount_question,
    _trend_question,
    _weekday_weekend_question,
    _year_over_year_compare,
)
from .planner_text_utils import _contains_any, _extract_top_k
from .planner_time import (
    _available_time_buckets,
    _build_month_range_filters,
    _extract_date_literal,
    _extract_recent_period_count,
    _extract_month_literal,
    _extract_time_grain,
    _extract_week_literal,
    _previous_period_literal,
    _resolve_recent_period_buckets,
    _resolve_forecast_targets,
    _resolve_requested_single_month_bucket,
    _time_bucket_alias,
)
from ..core.schema import ChartSpec, DerivedColumn, Filter, FormulaMetric, Metric, PivotSpec, SelectionPlan, Sort, TransformPlan
from .planner_types import HeuristicActionRuntimeContext


def _find_column_by_hint(profiles: dict[str, dict[str, Any]], hint: str) -> str | None:
    for column, profile in profiles.items():
        if hint in (profile.get("semantic_hints") or []):
            return column
    return None


def _is_short_ascii_token(token: str) -> bool:
    text = str(token or "")
    return len(text) < 3 and text.isascii()


def _question_value_filters(
    df: Any,
    *,
    question: str,
    profiles: dict[str, dict[str, Any]],
    exclude_columns: set[str] | None = None,
    max_filters: int = 3,
) -> list[tuple[str, str]]:
    exclude_columns = exclude_columns or set()
    normalized_question = normalize_text(question)
    if not normalized_question:
        return []

    def column_priority(column: str, profile: dict[str, Any]) -> tuple[int, float]:
        normalized = normalize_text(column)
        semantic_type = str(profile.get("semantic_type") or "")
        unique_ratio = float(profile.get("unique_ratio") or 0.0)
        score = 0
        if any(token in normalized for token in ("region", "地区", "地域", "区域")):
            score += 6
        if any(token in normalized for token in ("service", "服务")):
            score += 5
        if any(token in normalized for token in ("item", "category", "分类", "类别")):
            score += 4
        if semantic_type == "categorical":
            score += 2
        if semantic_type == "id" and unique_ratio > 0.9:
            score -= 2
        return score, unique_ratio

    candidates: list[tuple[int, float, str, str]] = []
    for column, profile in profiles.items():
        if column in exclude_columns:
            continue
        if column not in getattr(df, "columns", []):
            continue
        semantic_type = str(profile.get("semantic_type") or "")
        if semantic_type in {"numeric"}:
            continue

        values = [str(item).strip() for item in df[column].dropna().head(400).tolist()]
        matched_value = ""
        for value in values:
            normalized_value = normalize_text(value)
            if not normalized_value:
                continue
            if len(normalized_value) < 2 or _is_short_ascii_token(normalized_value):
                continue
            if normalized_value in normalized_question:
                if len(normalized_value) > len(normalize_text(matched_value)):
                    matched_value = value
        if not matched_value:
            continue

        score, unique_ratio = column_priority(column, profile)
        candidates.append((score, unique_ratio, column, matched_value))

    if not candidates:
        return []

    candidates.sort(key=lambda item: (item[0], -item[1], len(item[3])), reverse=True)
    selected: list[tuple[str, str]] = []
    seen_columns: set[str] = set()
    for _score, _unique_ratio, column, value in candidates:
        if column in seen_columns:
            continue
        seen_columns.add(column)
        selected.append((column, value))
        if len(selected) >= max_filters:
            break
    return selected


def _merge_selection_filters(selection_plan: SelectionPlan, filters: list[Filter]) -> SelectionPlan:
    if not filters:
        return selection_plan
    existing = [flt for flt in (selection_plan.filters or []) if str(flt.col) not in {str(item.col) for item in filters}]
    return selection_plan.model_copy(update={"filters": [*existing, *filters]})


def _selection_with_question_filters(
    df: Any,
    *,
    selection_plan: SelectionPlan,
    question: str,
    profiles: dict[str, dict[str, Any]],
    exclude_columns: set[str] | None = None,
) -> tuple[SelectionPlan, list[dict[str, str]]]:
    matched_filters = _question_value_filters(
        df,
        question=question,
        profiles=profiles,
        exclude_columns=exclude_columns,
    )
    if not matched_filters:
        return selection_plan, []
    plan_filters = [Filter(col=column, op="=", value=value) for column, value in matched_filters]
    merged = _merge_selection_filters(selection_plan, plan_filters)
    return merged, [{"column": column, "value": value} for column, value in matched_filters]


def _compare_basis(chat_text: str) -> str:
    return "year_over_year" if _year_over_year_compare(chat_text) else "previous_period"


def _compare_type(chat_text: str, *, basis: str) -> str:
    if _ratio_compare(chat_text):
        return "ratio"
    if _delta_compare(chat_text):
        return "delta"
    if basis == "year_over_year":
        return "yoy"
    return "mom"


def _explicit_compare_period(chat_text: str, *, grain: str) -> str | None:
    if grain == "day":
        return _extract_date_literal(chat_text)
    if grain == "week":
        return _extract_week_literal(chat_text)
    if grain == "month":
        return _extract_month_literal(chat_text)
    return None


def _period_compare_question(chat_text: str) -> bool:
    text = str(chat_text or "")
    if not _compare_question(text):
        return False
    has_explicit_period = bool(_extract_month_literal(text) or _extract_date_literal(text) or _extract_week_literal(text))
    if has_explicit_period or _year_over_year_compare(text):
        return True
    return _contains_any(
        text,
        (
            "环比",
            "上个月",
            "上月",
            "本月",
            "这个月",
            "按月",
            "每月",
            "去年",
            "同期",
            "last month",
            "previous month",
            "month over month",
            "mom",
            "year over year",
            "yoy",
            "per month",
            "monthly",
        ),
    )


def _resolve_compare_periods(
    df: Any,
    *,
    date_column: str,
    chat_text: str,
    grain: str,
    basis: str,
) -> tuple[str, str] | None:
    available = sorted(_available_time_buckets(df, date_column=date_column, grain=grain))
    if not available:
        return None

    current_period = _explicit_compare_period(chat_text, grain=grain)
    if not current_period or current_period not in available:
        current_period = available[-1]

    previous_period = _previous_period_literal(current_period, grain=grain, basis=basis)
    if not previous_period:
        return None

    if previous_period not in available:
        if basis == "previous_period":
            try:
                current_index = available.index(current_period)
            except ValueError:
                current_index = -1
            if current_index <= 0:
                return None
            previous_period = available[current_index - 1]
        else:
            return None

    if previous_period == current_period:
        return None
    return current_period, previous_period


def _explicit_chart_type(chat_text: str) -> str | None:
    text = str(chat_text or "").lower()
    if any(token in text for token in ("柱状图", "条形图")) or re.search(r"\bbar\b", text, flags=re.I):
        return "bar"
    if "饼图" in text or re.search(r"\bpie\b", text, flags=re.I):
        return "pie"
    if "折线图" in text or re.search(r"\bline\b", text, flags=re.I):
        return "line"
    if "散点图" in text or re.search(r"\bscatter\b", text, flags=re.I):
        return "scatter"
    return None


def _infer_chart_unit(column: str, profiles: dict[str, dict[str, Any]]) -> str:
    raw = str(column or "")
    normalized = raw.lower()
    profile = profiles.get(raw) or {}
    hints = [str(item) for item in (profile.get("semantic_hints") or [])]
    if "%" in raw or "％" in raw or "percent" in normalized:
        return "%"
    if any(token in raw for token in ("元", "万元", "亿元", "¥", "￥")):
        return "CNY"
    if any(token in normalized for token in ("usd", "$")):
        return "USD"
    if any(token in normalized for token in ("cny", "rmb")):
        return "CNY"
    bracket_match = re.search(r"[（(]([^()（）]{1,12})[）)]", raw)
    if bracket_match:
        return str(bracket_match.group(1)).strip()
    if "amount" in hints:
        return "amount"
    if str(profile.get("semantic_type") or "") == "numeric":
        return "value"
    return ""


def _recommended_chart_type(
    intent: str,
    *,
    fallback_type: str,
    top_k: int | None,
    x_column: str,
    profiles: dict[str, dict[str, Any]],
) -> str:
    x_profile = profiles.get(x_column) or {}
    x_type = str(x_profile.get("semantic_type") or "")
    if intent == "trend":
        return "line"
    if intent == "share":
        if x_type in {"numeric", "date"}:
            return "bar"
        if isinstance(top_k, int) and top_k > 8:
            return "bar"
        return "pie"
    if intent in {"ranking", "weekpart_compare", "period_breakdown"}:
        return "line" if x_type == "date" else "bar"
    if fallback_type == "pie" and x_type in {"numeric", "date"}:
        return "bar"
    return fallback_type


def _default_chart_title(intent: str, *, x_column: str, y_column: str, chart_type: str) -> str:
    base = {
        "ranking": "Ranking",
        "share": "Share",
        "trend": "Trend",
        "weekpart_compare": "Weekday vs weekend",
        "period_breakdown": "Period breakdown",
    }.get(intent, chart_type.title())
    return f"{base}: {y_column} by {x_column}"


def _build_chart_spec_with_context(
    *,
    chat_text: str,
    intent: str,
    default_type: str,
    x_column: str,
    y_column: str,
    metric_source_column: str | None = None,
    top_k: int | None,
    profiles: dict[str, dict[str, Any]],
) -> tuple[ChartSpec, dict[str, Any]]:
    explicit_type = _explicit_chart_type(chat_text)
    requested_type = explicit_type or default_type
    recommended_type = _recommended_chart_type(
        intent,
        fallback_type=requested_type,
        top_k=top_k,
        x_column=x_column,
        profiles=profiles,
    )
    applied_type = requested_type if explicit_type else recommended_type
    title = str(chat_text or "").strip()
    if not title or len(title) > 72:
        title = _default_chart_title(intent, x_column=x_column, y_column=y_column, chart_type=applied_type)
    y_unit = _infer_chart_unit(metric_source_column or y_column, profiles)
    return (
        ChartSpec(type=applied_type, title=title, x=x_column, y=y_column, top_k=top_k),
        {
            "title": title,
            "x_label": x_column,
            "y_label": y_column,
            "y_unit": y_unit,
            "requested_type": requested_type,
            "recommended_type": recommended_type,
            "applied_type": applied_type,
            "explicit_type": bool(explicit_type),
        },
    )


def build_period_compare_plan(
    df: Any,
    *,
    chat_text: str,
    date_column: str | None,
    amount_column: str | None,
    profiles: dict[str, dict[str, Any]],
    planner_meta: dict[str, Any],
    plan_draft_factory: Callable[..., Any],
) -> Any | None:
    if not _period_compare_question(chat_text) or not date_column:
        return None

    compare_metric_column = amount_column
    if not compare_metric_column:
        return None

    grain = _extract_time_grain(chat_text, default="month")
    if grain not in {"day", "week", "month"}:
        grain = "month"
    basis = _compare_basis(chat_text)
    comparison_type = _compare_type(chat_text, basis=basis)
    period_pair = _resolve_compare_periods(
        df,
        date_column=date_column,
        chat_text=chat_text,
        grain=grain,
        basis=basis,
    )
    if period_pair is None:
        return None
    current_period, previous_period = period_pair

    bucket_name = _time_bucket_alias(grain)
    post_pivot_metrics = [
        FormulaMetric(as_name="change_value", op="sub", left=current_period, right=previous_period),
        FormulaMetric(as_name="change_pct", op="div", left="change_value", right=previous_period),
    ]
    if comparison_type == "ratio":
        post_pivot_metrics.append(FormulaMetric(as_name="compare_ratio", op="div", left=current_period, right=previous_period))

    selection_plan, value_filters = _selection_with_question_filters(
        df,
        selection_plan=SelectionPlan(columns=[date_column, compare_metric_column]),
        question=chat_text,
        profiles=profiles,
        exclude_columns={date_column, compare_metric_column},
    )

    return plan_draft_factory(
        mode="text",
        intent="period_compare",
        selection_plan=selection_plan,
        transform_plan=TransformPlan(
            derived_columns=[DerivedColumn(as_name=bucket_name, kind="date_bucket", source_col=date_column, grain=grain)],
            groupby=[bucket_name],
            metrics=[Metric(agg="sum", col=compare_metric_column, as_name="value")],
            having=[Filter(col=bucket_name, op="in", value=[previous_period, current_period])],
            pivot=PivotSpec(index=[], columns=bucket_name, values="value", fill_value=0),
            post_pivot_formula_metrics=post_pivot_metrics,
        ),
        planner_meta={
            **planner_meta,
            "compare_basis": basis,
            "comparison_type": comparison_type,
            "compare_grain": grain,
            "compare_metric_column": compare_metric_column,
            "current_period": current_period,
            "previous_period": previous_period,
            "compare_window": [previous_period, current_period],
            **({"value_filters": value_filters} if value_filters else {}),
        },
    )


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
        selection_plan, value_filters = _selection_with_question_filters(
            df,
            selection_plan=SelectionPlan(
                columns=[date_column, amount_column],
                filters=_build_month_range_filters(date_column, requested_single_month),
            ),
            question=effective_chat_text,
            profiles=profiles,
            exclude_columns={date_column, amount_column},
        )
        return plan_draft_factory(
            mode="text",
            intent="total_amount",
            selection_plan=selection_plan,
            transform_plan=TransformPlan(metrics=[Metric(agg="sum", col=amount_column, as_name="total_amount")]),
            planner_meta={**planner_meta, "requested_period": requested_single_month, **({"value_filters": value_filters} if value_filters else {})},
        )

    requested_months = _multi_period_amount_question(df, chat_text=effective_chat_text, date_column=date_column)
    if requested_months and amount_column and date_column and not preserve_previous_analysis:
        bucket_name = _time_bucket_alias("month")
        chart_spec: ChartSpec | None = None
        chart_context: dict[str, Any] = {}
        if mode == "chart":
            chart_spec, chart_context = _build_chart_spec_with_context(
                chat_text=chat_text,
                intent="period_breakdown",
                default_type="bar",
                x_column=bucket_name,
                y_column="value",
                metric_source_column=amount_column,
                top_k=len(requested_months),
                profiles=profiles,
            )
        selection_plan, value_filters = _selection_with_question_filters(
            df,
            selection_plan=SelectionPlan(columns=[date_column, amount_column]),
            question=effective_chat_text,
            profiles=profiles,
            exclude_columns={date_column, amount_column},
        )
        return plan_draft_factory(
            mode=mode,
            intent="period_breakdown",
            selection_plan=selection_plan,
            transform_plan=TransformPlan(
                derived_columns=[DerivedColumn(as_name=bucket_name, kind="date_bucket", source_col=date_column, grain="month")],
                groupby=[bucket_name],
                metrics=[Metric(agg="sum", col=amount_column, as_name="value")],
                having=[Filter(col=bucket_name, op="in", value=requested_months)],
                order_by=Sort(col=bucket_name, dir="asc"),
            ),
            chart_spec=chart_spec,
            planner_meta={
                **planner_meta,
                "requested_months": requested_months,
                "bucket_name": bucket_name,
                **({"chart_context": chart_context} if chart_context else {}),
                **({"value_filters": value_filters} if value_filters else {}),
            },
        )

    if _total_amount_question(effective_chat_text) and amount_column and not preserve_previous_analysis:
        selection_plan, value_filters = _selection_with_question_filters(
            df,
            selection_plan=SelectionPlan(columns=[amount_column]),
            question=effective_chat_text,
            profiles=profiles,
            exclude_columns={amount_column},
        )
        return plan_draft_factory(
            mode="text",
            intent="total_amount",
            selection_plan=selection_plan,
            transform_plan=TransformPlan(metrics=[Metric(agg="sum", col=amount_column, as_name="total_amount")]),
            planner_meta={**planner_meta, **({"value_filters": value_filters} if value_filters else {})},
        )

    if _average_amount_question(effective_chat_text) and amount_column and not preserve_previous_analysis:
        selection_plan, value_filters = _selection_with_question_filters(
            df,
            selection_plan=SelectionPlan(columns=[amount_column]),
            question=effective_chat_text,
            profiles=profiles,
            exclude_columns={amount_column},
        )
        return plan_draft_factory(
            mode="text",
            intent="average_amount",
            selection_plan=selection_plan,
            transform_plan=TransformPlan(metrics=[Metric(agg="avg", col=amount_column, as_name="avg_amount")]),
            planner_meta={**planner_meta, **({"value_filters": value_filters} if value_filters else {})},
        )

    if _weekday_weekend_question(effective_chat_text) and amount_column and date_column:
        chart_spec: ChartSpec | None = None
        chart_context: dict[str, Any] = {}
        if mode == "chart":
            requested_chart = _extract_chart_type(effective_chat_text, default="bar")
            chart_spec, chart_context = _build_chart_spec_with_context(
                chat_text=chat_text,
                intent="weekpart_compare",
                default_type=requested_chart,
                x_column="weekpart",
                y_column="value",
                metric_source_column=amount_column,
                top_k=2,
                profiles=profiles,
            )
        selection_plan, value_filters = _selection_with_question_filters(
            df,
            selection_plan=SelectionPlan(columns=[date_column, amount_column]),
            question=effective_chat_text,
            profiles=profiles,
            exclude_columns={amount_column},
        )
        return plan_draft_factory(
            mode=mode,
            intent="weekpart_compare",
            selection_plan=selection_plan,
            transform_plan=TransformPlan(
                derived_columns=[DerivedColumn(as_name="weekpart", kind="date_bucket", source_col=date_column, grain="weekpart")],
                groupby=["weekpart"],
                metrics=[Metric(agg="sum", col=amount_column, as_name="value")],
                order_by=Sort(col="value", dir="desc"),
                top_k=2,
            ),
            chart_spec=chart_spec,
            planner_meta={**planner_meta, **({"chart_context": chart_context} if chart_context else {}), **({"value_filters": value_filters} if value_filters else {})},
        )

    if not preserve_previous_analysis and not _single_transaction_question(effective_chat_text) and not _trend_question(effective_chat_text):
        fast_path = try_chart_fast_path(df, question=effective_chat_text) if mode == "chart" else try_text_fast_path(df, question=effective_chat_text)
        if isinstance(fast_path, dict):
            selection_plan = fast_path["selection_plan"]
            value_filters: list[dict[str, str]] = []
            if isinstance(selection_plan, SelectionPlan):
                selection_plan, value_filters = _selection_with_question_filters(
                    df,
                    selection_plan=selection_plan,
                    question=effective_chat_text,
                    profiles=profiles,
                    exclude_columns={amount_column} if amount_column else set(),
                )
            fast_chart_spec = fast_path.get("chart_spec")
            chart_context: dict[str, Any] = {}
            if mode == "chart" and isinstance(fast_chart_spec, ChartSpec):
                fast_chart_spec, chart_context = _build_chart_spec_with_context(
                    chat_text=chat_text,
                    intent=str(fast_path.get("intent") or "chart"),
                    default_type=str(fast_chart_spec.type),
                    x_column=str(fast_chart_spec.x),
                    y_column=str(fast_chart_spec.y),
                    metric_source_column=amount_column if str(fast_chart_spec.y) == "value" else str(fast_chart_spec.y),
                    top_k=fast_chart_spec.top_k,
                    profiles=profiles,
                )
            return plan_draft_factory(
                mode=str(fast_path.get("mode") or mode),
                intent=str(fast_path.get("intent") or "unsupported"),
                selection_plan=selection_plan,
                transform_plan=fast_path["transform_plan"],
                chart_spec=fast_chart_spec if isinstance(fast_chart_spec, ChartSpec) else fast_path.get("chart_spec"),
                planner_meta={
                    **planner_meta,
                    **(fast_path.get("planner_meta") or {}),
                    **({"chart_context": chart_context} if chart_context else {}),
                    **({"value_filters": value_filters} if value_filters else {}),
                },
            )

    if (_detail_to_ranking_followup(chat_text, followup_context) or _item_ranking_question(effective_chat_text)) and amount_column:
        ranking_column = item_preferred_column or item_column or question_dimension_column or category_column or single_transaction_column
        if ranking_column:
            top_k = _extract_top_k(effective_chat_text, default=5)
            ranking_mode = "chart" if mode == "chart" else "text"
            chart_spec: ChartSpec | None = None
            chart_context: dict[str, Any] = {}
            if ranking_mode == "chart":
                requested_chart = _extract_chart_type(effective_chat_text, default="bar")
                chart_spec, chart_context = _build_chart_spec_with_context(
                    chat_text=chat_text,
                    intent="ranking",
                    default_type=requested_chart,
                    x_column=ranking_column,
                    y_column="value",
                    metric_source_column=amount_column,
                    top_k=top_k,
                    profiles=profiles,
                )
            selection_plan, value_filters = _selection_with_question_filters(
                df,
                selection_plan=SelectionPlan(columns=[ranking_column, amount_column]),
                question=effective_chat_text,
                profiles=profiles,
                exclude_columns={amount_column},
            )
            return plan_draft_factory(
                mode=ranking_mode,
                intent="ranking",
                selection_plan=selection_plan,
                transform_plan=TransformPlan(
                    groupby=[ranking_column],
                    metrics=[Metric(agg="sum", col=amount_column, as_name="value")],
                    order_by=Sort(col="value", dir="desc"),
                    top_k=top_k,
                ),
                chart_spec=chart_spec,
                planner_meta={
                    **planner_meta,
                    "top_k": top_k,
                    "ranking_column": ranking_column,
                    **({"chart_context": chart_context} if chart_context else {}),
                    **({"value_filters": value_filters} if value_filters else {}),
                },
            )

    if _single_transaction_question(effective_chat_text) and amount_column:
        top_k = _extract_top_k(effective_chat_text, default=5)
        if mode == "chart" and single_transaction_column:
            requested_chart = _extract_chart_type(effective_chat_text, default="bar")
            chart_spec, chart_context = _build_chart_spec_with_context(
                chat_text=chat_text,
                intent="ranking",
                default_type=requested_chart,
                x_column=single_transaction_column,
                y_column="value",
                metric_source_column=amount_column,
                top_k=top_k,
                profiles=profiles,
            )
            selection_plan, value_filters = _selection_with_question_filters(
                df,
                selection_plan=SelectionPlan(columns=[single_transaction_column, amount_column]),
                question=effective_chat_text,
                profiles=profiles,
                exclude_columns={amount_column},
            )
            return plan_draft_factory(
                mode="chart",
                intent="ranking",
                selection_plan=selection_plan,
                transform_plan=TransformPlan(
                    groupby=[single_transaction_column],
                    metrics=[Metric(agg="sum", col=amount_column, as_name="value")],
                    order_by=Sort(col="value", dir="desc"),
                    top_k=top_k,
                ),
                chart_spec=chart_spec,
                planner_meta={**planner_meta, "top_k": top_k, **({"chart_context": chart_context} if chart_context else {}), **({"value_filters": value_filters} if value_filters else {})},
            )
        selection_plan, value_filters = _selection_with_question_filters(
            df,
            selection_plan=SelectionPlan(sort=Sort(col=amount_column, dir="desc"), limit=top_k),
            question=effective_chat_text,
            profiles=profiles,
            exclude_columns={amount_column},
        )
        return plan_draft_factory(
            mode="text",
            intent="detail_rows",
            selection_plan=selection_plan,
            transform_plan=TransformPlan(return_rows=True),
            planner_meta={**planner_meta, "top_k": top_k, **({"value_filters": value_filters} if value_filters else {})},
        )

    if _trend_question(effective_chat_text) and amount_column and date_column:
        bucket_grain = _extract_time_grain(effective_chat_text, default="month")
        bucket_name = _time_bucket_alias(bucket_grain)
        requested_recent_count = _extract_recent_period_count(effective_chat_text, grain=bucket_grain)
        recent_periods = (
            _resolve_recent_period_buckets(df, date_column=date_column, grain=bucket_grain, count=requested_recent_count)
            if isinstance(requested_recent_count, int) and requested_recent_count > 0
            else []
        )
        having_filters = [Filter(col=bucket_name, op="in", value=recent_periods)] if recent_periods else []
        transform_plan = TransformPlan(
            derived_columns=[DerivedColumn(as_name=bucket_name, kind="date_bucket", source_col=date_column, grain=bucket_grain)],
            groupby=[bucket_name],
            metrics=[Metric(agg="sum", col=amount_column, as_name="value")],
            having=having_filters,
            order_by=Sort(col=bucket_name, dir="asc"),
        )
        selection_plan, value_filters = _selection_with_question_filters(
            df,
            selection_plan=SelectionPlan(columns=[date_column, amount_column]),
            question=effective_chat_text,
            profiles=profiles,
            exclude_columns={amount_column},
        )
        requested_single_month = (
            _resolve_requested_single_month_bucket(df, date_column=date_column, chat_text=effective_chat_text)
            if bucket_grain == "day"
            else None
        )
        if requested_single_month:
            selection_plan = _merge_selection_filters(selection_plan, _build_month_range_filters(date_column, requested_single_month))
        chart_spec: ChartSpec | None = None
        chart_context: dict[str, Any] = {}
        if mode == "chart":
            chart_spec, chart_context = _build_chart_spec_with_context(
                chat_text=chat_text,
                intent="trend",
                default_type="line",
                x_column=bucket_name,
                y_column="value",
                metric_source_column=amount_column,
                top_k=24,
                profiles=profiles,
            )
        return plan_draft_factory(
            mode=mode,
            intent="trend",
            selection_plan=selection_plan,
            transform_plan=transform_plan,
            chart_spec=chart_spec,
            planner_meta={
                **planner_meta,
                "bucket_name": bucket_name,
                "bucket_grain": bucket_grain,
                **({"chart_context": chart_context} if chart_context else {}),
                **({"requested_recent_period_count": requested_recent_count} if requested_recent_count else {}),
                **({"requested_recent_periods": recent_periods} if recent_periods else {}),
                **({"requested_period": requested_single_month} if requested_single_month else {}),
                **({"value_filters": value_filters} if value_filters else {}),
            },
        )

    if _share_question(effective_chat_text) and category_column:
        share_column = question_dimension_column or category_column
        metric_column = amount_column
        metric = Metric(agg="sum", col=metric_column, as_name="value") if metric_column else Metric(agg="count_rows", as_name="value")
        top_k = _extract_top_k(effective_chat_text, 8)
        share_mode = "chart" if mode == "chart" else "text"
        chart_spec: ChartSpec | None = None
        chart_context: dict[str, Any] = {}
        if share_mode == "chart":
            requested_chart = _extract_chart_type(effective_chat_text, default="pie")
            chart_spec, chart_context = _build_chart_spec_with_context(
                chat_text=chat_text,
                intent="share",
                default_type=requested_chart,
                x_column=share_column,
                y_column="value",
                metric_source_column=metric_column,
                top_k=top_k,
                profiles=profiles,
            )
        selection_plan, value_filters = _selection_with_question_filters(
            df,
            selection_plan=SelectionPlan(columns=[share_column] + ([metric_column] if metric_column else [])),
            question=effective_chat_text,
            profiles=profiles,
            exclude_columns={metric_column} if metric_column else set(),
        )
        return plan_draft_factory(
            mode=share_mode,
            intent="share",
            selection_plan=selection_plan,
            transform_plan=TransformPlan(groupby=[share_column], metrics=[metric], order_by=Sort(col="value", dir="desc"), top_k=top_k),
            chart_spec=chart_spec,
            planner_meta={
                **planner_meta,
                "share_column": share_column,
                "top_k": top_k,
                **({"chart_context": chart_context} if chart_context else {}),
                **({"value_filters": value_filters} if value_filters else {}),
            },
        )

    if _detail_question(effective_chat_text) and amount_column:
        top_k = _extract_top_k(effective_chat_text, default=10)
        selection_plan, value_filters = _selection_with_question_filters(
            df,
            selection_plan=SelectionPlan(sort=Sort(col=amount_column, dir="desc"), limit=top_k),
            question=effective_chat_text,
            profiles=profiles,
            exclude_columns={amount_column},
        )
        return plan_draft_factory(
            mode="text",
            intent="detail_rows",
            selection_plan=selection_plan,
            transform_plan=TransformPlan(return_rows=True),
            planner_meta={**planner_meta, "top_k": top_k, **({"value_filters": value_filters} if value_filters else {})},
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
        chart_spec: ChartSpec | None = None
        chart_context: dict[str, Any] = {}
        if mode == "chart":
            requested_chart = _extract_chart_type(effective_chat_text, default="bar")
            chart_spec, chart_context = _build_chart_spec_with_context(
                chat_text=chat_text,
                intent="ranking",
                default_type=requested_chart,
                x_column=ranking_column,
                y_column="value",
                metric_source_column=metric_column,
                top_k=top_k,
                profiles=profiles,
            )
        selection_plan, value_filters = _selection_with_question_filters(
            df,
            selection_plan=SelectionPlan(columns=[ranking_column] + ([metric_column] if metric_column else [])),
            question=effective_chat_text,
            profiles=profiles,
            exclude_columns={metric_column} if metric_column else set(),
        )
        return plan_draft_factory(
            mode=mode,
            intent="ranking",
            selection_plan=selection_plan,
            transform_plan=TransformPlan(groupby=[ranking_column], metrics=[metric], order_by=Sort(col="value", dir="desc"), top_k=top_k),
            chart_spec=chart_spec,
            planner_meta={
                **planner_meta,
                "ranking_column": ranking_column,
                "top_k": top_k,
                **({"chart_context": chart_context} if chart_context else {}),
                **({"value_filters": value_filters} if value_filters else {}),
            },
        )

    return None
