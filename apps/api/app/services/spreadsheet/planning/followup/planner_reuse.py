from __future__ import annotations

from typing import Any, Callable

from .planner_followup_context import _breakdown_followup, _explain_request
from .reuse_analysis import (
    _explanation_columns,
    _reuse_previous_plan_as_explain_breakdown,
    _reuse_previous_plan_as_period_compare,
    _reuse_previous_plan_with_value_filters,
)
from .reuse_base import (
    _previous_ranking_target_from_question,
)
from .reuse_view import (
    _reuse_previous_plan_as_detail,
    _reuse_previous_plan_as_share,
    _reuse_previous_plan_as_trend,
    _reuse_previous_plan_for_rank_position,
    _reuse_previous_plan_with_dimension,
    _reuse_previous_plan_with_mode_switch,
    _reuse_previous_plan_with_time_filter,
    _reuse_previous_plan_with_top_k,
)
from ..planner_types import FollowupSignalResolvers, ReuseFollowupRuntimeContext
from ...core.schema import Filter, SelectionPlan, Sort, TransformPlan


def _finalize_reuse_draft(draft: Any, planner_meta: dict[str, Any], *, reuse_strategy: str) -> Any:
    draft.planner_meta = {
        **planner_meta,
        **draft.planner_meta,
        "followup_reused_previous_plan": True,
        "reuse_strategy": reuse_strategy,
    }
    return draft


def try_reuse_followup_plan(
    df: Any,
    *,
    chat_text: str,
    runtime_context: ReuseFollowupRuntimeContext,
    signal_resolvers: FollowupSignalResolvers,
    plan_draft_factory: Callable[..., Any],
) -> Any | None:
    mode = runtime_context.mode
    amount_column = runtime_context.amount_column
    date_column = runtime_context.date_column
    raw_question_dimension_column = runtime_context.raw_question_dimension_column
    question_dimension_column = runtime_context.question_dimension_column
    service_column = runtime_context.service_column
    region_column = runtime_context.region_column
    item_column = runtime_context.item_column
    item_preferred_column = runtime_context.item_preferred_column
    category_column = runtime_context.category_column
    followup_context = runtime_context.followup_context
    planner_meta = runtime_context.planner_meta
    profiles = runtime_context.profiles

    reused_followup = _reuse_previous_plan_with_top_k(
        chat_text,
        mode=mode,
        followup_context=followup_context,
        top_k_followup=signal_resolvers.top_k_followup,
        plan_draft_factory=plan_draft_factory,
    )
    if reused_followup is not None:
        return _finalize_reuse_draft(reused_followup, planner_meta, reuse_strategy="top_k")

    switched_mode_followup = _reuse_previous_plan_with_mode_switch(
        chat_text,
        mode=mode,
        followup_context=followup_context,
        mode_switch_followup=signal_resolvers.mode_switch_followup,
        plan_draft_factory=plan_draft_factory,
    )
    if switched_mode_followup is not None:
        return _finalize_reuse_draft(switched_mode_followup, planner_meta, reuse_strategy="mode_switch")

    ranked_item_followup = _reuse_previous_plan_for_rank_position(
        df,
        chat_text,
        followup_context=followup_context,
        rank_lookup_followup=signal_resolvers.rank_lookup_followup,
        plan_draft_factory=plan_draft_factory,
    )
    if ranked_item_followup is not None:
        return _finalize_reuse_draft(ranked_item_followup, planner_meta, reuse_strategy="rank_lookup")

    if _explain_request(chat_text) and not _breakdown_followup(chat_text, followup_context):
        previous_target = _previous_ranking_target_from_question(
            df,
            chat_text,
            followup_context,
            rank_position_from_text=signal_resolvers.rank_position_from_text,
        )
        if previous_target is not None:
            dimension_column, target_value, _metric_alias, explain_rank_position = previous_target
            explain_columns = _explanation_columns(
                profiles,
                dimension_column=dimension_column,
                amount_column=amount_column,
                date_column=date_column,
            )
            selection_plan = SelectionPlan(
                columns=explain_columns,
                filters=[Filter(col=dimension_column, op="=", value=target_value)],
                sort=Sort(col=amount_column, dir="desc") if amount_column else None,
                limit=5,
            )
            return _finalize_reuse_draft(
                plan_draft_factory(
                    mode="text",
                    intent="explain_ranked_item",
                    selection_plan=selection_plan,
                    transform_plan=TransformPlan(return_rows=True),
                    planner_meta={
                        "explain_dimension_column": dimension_column,
                        "explain_target_value": target_value,
                        "explain_rank_position": explain_rank_position,
                    },
                ),
                planner_meta,
                reuse_strategy="explain_ranked_item",
            )

    share_followup = _reuse_previous_plan_as_share(
        chat_text,
        mode=mode,
        followup_context=followup_context,
        share_switch_followup=signal_resolvers.share_switch_followup,
        plan_draft_factory=plan_draft_factory,
    )
    if share_followup is not None:
        return _finalize_reuse_draft(share_followup, planner_meta, reuse_strategy="share_switch")

    dimension_followup = _reuse_previous_plan_with_dimension(
        chat_text,
        mode=mode,
        new_dimension=raw_question_dimension_column,
        followup_context=followup_context,
        dimension_switch_followup=signal_resolvers.dimension_switch_followup,
        plan_draft_factory=plan_draft_factory,
    )
    if dimension_followup is not None:
        return _finalize_reuse_draft(dimension_followup, planner_meta, reuse_strategy="dimension_switch")

    trend_followup = _reuse_previous_plan_as_trend(
        chat_text,
        mode=mode,
        date_column=date_column,
        followup_context=followup_context,
        trend_switch_followup=signal_resolvers.trend_switch_followup,
        plan_draft_factory=plan_draft_factory,
    )
    if trend_followup is not None:
        return _finalize_reuse_draft(trend_followup, planner_meta, reuse_strategy="trend_switch")

    detail_followup = _reuse_previous_plan_as_detail(
        chat_text,
        amount_column=amount_column,
        followup_context=followup_context,
        detail_switch_followup=signal_resolvers.detail_switch_followup,
        plan_draft_factory=plan_draft_factory,
    )
    if detail_followup is not None:
        return _finalize_reuse_draft(detail_followup, planner_meta, reuse_strategy="detail_switch")

    compare_followup = _reuse_previous_plan_as_period_compare(
        df,
        chat_text,
        date_column=date_column,
        amount_column=amount_column,
        followup_context=followup_context,
        plan_draft_factory=plan_draft_factory,
    )
    if compare_followup is not None:
        return _finalize_reuse_draft(compare_followup, planner_meta, reuse_strategy="period_compare")

    explain_breakdown_followup = _reuse_previous_plan_as_explain_breakdown(
        df,
        chat_text,
        amount_column=amount_column,
        new_dimension=raw_question_dimension_column,
        followup_context=followup_context,
        rank_position_from_text=signal_resolvers.rank_position_from_text,
        breakdown_followup=_breakdown_followup,
        plan_draft_factory=plan_draft_factory,
    )
    if explain_breakdown_followup is not None:
        return _finalize_reuse_draft(explain_breakdown_followup, planner_meta, reuse_strategy="explain_breakdown")

    value_filter_followup = _reuse_previous_plan_with_value_filters(
        df,
        chat_text,
        mode=mode,
        candidate_columns=runtime_context.candidate_columns,
        date_column=date_column,
        followup_context=followup_context,
        time_filter_followup=signal_resolvers.time_filter_followup,
        plan_draft_factory=plan_draft_factory,
    )
    if value_filter_followup is not None:
        return _finalize_reuse_draft(value_filter_followup, planner_meta, reuse_strategy="value_filter")

    filtered_time_followup = _reuse_previous_plan_with_time_filter(
        chat_text,
        mode=mode,
        date_column=date_column,
        followup_context=followup_context,
        time_filter_followup=signal_resolvers.time_filter_followup,
        plan_draft_factory=plan_draft_factory,
    )
    if filtered_time_followup is not None:
        return _finalize_reuse_draft(filtered_time_followup, planner_meta, reuse_strategy="time_filter")

    return None
