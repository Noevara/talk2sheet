from __future__ import annotations

from .followup.planner_followup_context import (
    _detail_switch_followup,
    _dimension_switch_followup,
    _mode_switch_followup,
    _rank_lookup_followup,
    _rank_position_from_text,
    _share_switch_followup,
    _time_filter_followup,
    _top_k_followup,
    _trend_switch_followup,
)
from .planner_types import (
    FollowupSignalResolvers,
    HeuristicActionRuntimeContext,
    HeuristicPlanningContext,
    ReuseFollowupRuntimeContext,
)


def build_reuse_runtime_context(context: HeuristicPlanningContext) -> ReuseFollowupRuntimeContext:
    return ReuseFollowupRuntimeContext(
        mode=context.followup.mode,
        amount_column=context.columns.amount_column,
        date_column=context.columns.date_column,
        raw_question_dimension_column=context.columns.raw_question_dimension_column,
        question_dimension_column=context.columns.question_dimension_column,
        service_column=context.columns.service_column,
        region_column=context.columns.region_column,
        item_column=context.columns.item_column,
        item_preferred_column=context.columns.item_preferred_column,
        category_column=context.columns.category_column,
        followup_context=context.followup.followup_context,
        planner_meta=context.planner_meta,
        profiles=context.profiles,
    )


def build_action_runtime_context(context: HeuristicPlanningContext) -> HeuristicActionRuntimeContext:
    return HeuristicActionRuntimeContext(
        effective_chat_text=context.followup.effective_chat_text,
        mode=context.followup.mode,
        followup_context=context.followup.followup_context,
        preserve_previous_analysis=context.followup.preserve_previous_analysis,
        profiles=context.profiles,
        planner_meta=context.planner_meta,
        amount_column=context.columns.amount_column,
        date_column=context.columns.date_column,
        category_column=context.columns.category_column,
        single_transaction_column=context.columns.single_transaction_column,
        item_preferred_column=context.columns.item_preferred_column,
        item_column=context.columns.item_column,
        question_dimension_column=context.columns.question_dimension_column,
    )


def build_followup_signal_resolvers() -> FollowupSignalResolvers:
    return FollowupSignalResolvers(
        rank_position_from_text=_rank_position_from_text,
        top_k_followup=_top_k_followup,
        mode_switch_followup=_mode_switch_followup,
        rank_lookup_followup=_rank_lookup_followup,
        share_switch_followup=_share_switch_followup,
        dimension_switch_followup=_dimension_switch_followup,
        trend_switch_followup=_trend_switch_followup,
        detail_switch_followup=_detail_switch_followup,
        time_filter_followup=_time_filter_followup,
    )
