from __future__ import annotations

from typing import Any, Callable

from ..conversation.context_interpreter import get_default_context_interpreter
from ..pipeline.column_profile import get_column_profiles
from .followup.planner_followup_context import (
    _effective_chat_text,
    _preserve_previous_analysis,
    _resolve_mode,
    _with_interpreted_followup,
)
from .planner_columns import (
    _find_amount_column,
    _find_category_column,
    _find_date_column,
    _find_item_column,
    _find_question_dimension_column,
    _find_region_column,
    _find_service_column,
    _find_single_transaction_group_column,
)
from .planner_types import FollowupPlanningState, HeuristicPlanningContext, ResolvedColumns


def _clarified_dimension_column(
    *,
    profiles: dict[str, dict[str, Any]],
    followup_context: dict[str, Any] | None,
    exclude: set[str] | None = None,
) -> str | None:
    exclude = exclude or set()
    if not isinstance(followup_context, dict):
        return None
    raw_resolution = followup_context.get("clarification_resolution")
    if not isinstance(raw_resolution, dict):
        return None
    selected_value = str(raw_resolution.get("selected_value") or "").strip()
    if not selected_value or selected_value in exclude:
        return None
    return selected_value if selected_value in profiles else None


def _build_followup_planning_state(
    *,
    chat_text: str,
    requested_mode: str,
    followup_context: dict[str, Any] | None,
    context_interpreter_factory: Callable[[], Any],
    df: Any,
) -> FollowupPlanningState:
    followup_context, context_interpreter_meta = _with_interpreted_followup(
        df,
        chat_text=chat_text,
        requested_mode=requested_mode,
        followup_context=followup_context,
        context_interpreter_factory=context_interpreter_factory,
    )
    effective_chat_text = _effective_chat_text(chat_text, followup_context)
    mode = _resolve_mode(chat_text, requested_mode, followup_context)
    preserve_previous_analysis = _preserve_previous_analysis(chat_text, followup_context)
    return FollowupPlanningState(
        followup_context=followup_context,
        context_interpreter_meta=context_interpreter_meta,
        effective_chat_text=effective_chat_text,
        mode=mode,
        preserve_previous_analysis=preserve_previous_analysis,
    )


def _resolve_core_columns(df: Any, profiles: dict[str, dict[str, Any]]) -> tuple[str | None, str | None, str | None, str | None]:
    amount_column = _find_amount_column(profiles)
    date_column = _find_date_column(profiles)
    category_column = _find_category_column(profiles, exclude={col for col in (amount_column, date_column) if col})
    single_transaction_column = _find_single_transaction_group_column(
        df,
        profiles,
        exclude={col for col in (amount_column,) if col},
    )
    return amount_column, date_column, category_column, single_transaction_column


def _resolve_named_dimension_columns(
    profiles: dict[str, dict[str, Any]],
    *,
    amount_column: str | None,
    date_column: str | None,
    single_transaction_column: str | None,
) -> tuple[str | None, str | None, str | None, str | None]:
    base_exclude = {col for col in (amount_column, date_column, single_transaction_column) if col}
    service_column = _find_service_column(profiles, exclude=base_exclude)
    region_column = _find_region_column(profiles, exclude=base_exclude)
    item_preferred_column = _find_item_column(profiles, exclude=base_exclude)
    item_column = _find_item_column(
        profiles,
        exclude={*base_exclude, *{col for col in (service_column, region_column) if col}},
    )
    return service_column, region_column, item_preferred_column, item_column


def _resolve_question_dimension_columns(
    profiles: dict[str, dict[str, Any]],
    *,
    chat_text: str,
    effective_chat_text: str,
    followup_context: dict[str, Any] | None,
    amount_column: str | None,
    date_column: str | None,
    single_transaction_column: str | None,
    item_column: str | None,
    service_column: str | None,
    region_column: str | None,
    category_column: str | None,
) -> tuple[str | None, str | None]:
    exclude = {col for col in (amount_column, date_column, single_transaction_column) if col}
    clarified_column = _clarified_dimension_column(
        profiles=profiles,
        followup_context=followup_context,
        exclude=exclude,
    )
    if clarified_column is not None:
        return clarified_column, clarified_column
    raw_question_dimension_column = _find_question_dimension_column(
        profiles,
        chat_text,
        exclude=exclude,
        item_column=item_column,
        service_column=service_column,
        region_column=region_column,
        category_column=category_column,
    )
    question_dimension_column = _find_question_dimension_column(
        profiles,
        effective_chat_text,
        exclude=exclude,
        item_column=item_column,
        service_column=service_column,
        region_column=region_column,
        category_column=category_column,
    )
    return raw_question_dimension_column, question_dimension_column


def _build_resolved_columns(
    df: Any,
    *,
    profiles: dict[str, dict[str, Any]],
    chat_text: str,
    effective_chat_text: str,
    followup_context: dict[str, Any] | None = None,
) -> ResolvedColumns:
    amount_column, date_column, category_column, single_transaction_column = _resolve_core_columns(df, profiles)
    service_column, region_column, item_preferred_column, item_column = _resolve_named_dimension_columns(
        profiles,
        amount_column=amount_column,
        date_column=date_column,
        single_transaction_column=single_transaction_column,
    )
    raw_question_dimension_column, question_dimension_column = _resolve_question_dimension_columns(
        profiles,
        chat_text=chat_text,
        effective_chat_text=effective_chat_text,
        followup_context=followup_context,
        amount_column=amount_column,
        date_column=date_column,
        single_transaction_column=single_transaction_column,
        item_column=item_column,
        service_column=service_column,
        region_column=region_column,
        category_column=category_column,
    )
    return ResolvedColumns(
        amount_column=amount_column,
        date_column=date_column,
        category_column=category_column,
        single_transaction_column=single_transaction_column,
        service_column=service_column,
        region_column=region_column,
        item_preferred_column=item_preferred_column,
        item_column=item_column,
        raw_question_dimension_column=raw_question_dimension_column,
        question_dimension_column=question_dimension_column,
    )


def _build_planner_meta(
    *,
    columns: ResolvedColumns,
    context_interpreter_meta: dict[str, Any],
    followup_context: dict[str, Any] | None,
) -> dict[str, Any]:
    planner_meta = {
        "planner": "heuristic-v1",
        "amount_column": columns.amount_column,
        "date_column": columns.date_column,
        "category_column": columns.category_column,
        "single_transaction_column": columns.single_transaction_column,
        "service_column": columns.service_column,
        "region_column": columns.region_column,
        "item_preferred_column": columns.item_preferred_column,
        "item_column": columns.item_column,
        "raw_question_dimension_column": columns.raw_question_dimension_column,
        "question_dimension_column": columns.question_dimension_column,
        "context_interpreter": context_interpreter_meta,
    }
    if followup_context:
        planner_meta["followup_context_used"] = True
        if isinstance(followup_context.get("clarification_resolution"), dict):
            planner_meta["clarification_resolution"] = dict(followup_context["clarification_resolution"])
    return planner_meta


def build_heuristic_planning_context(
    df: Any,
    *,
    chat_text: str,
    requested_mode: str,
    followup_context: dict[str, Any] | None,
    context_interpreter_factory: Callable[[], Any] = get_default_context_interpreter,
) -> HeuristicPlanningContext:
    followup = _build_followup_planning_state(
        chat_text=chat_text,
        requested_mode=requested_mode,
        followup_context=followup_context,
        context_interpreter_factory=context_interpreter_factory,
        df=df,
    )
    profiles = get_column_profiles(df)
    columns = _build_resolved_columns(
        df,
        profiles=profiles,
        chat_text=chat_text,
        effective_chat_text=followup.effective_chat_text,
        followup_context=followup.followup_context,
    )
    planner_meta = _build_planner_meta(
        columns=columns,
        context_interpreter_meta=followup.context_interpreter_meta,
        followup_context=followup.followup_context,
    )

    return HeuristicPlanningContext(
        followup=followup,
        columns=columns,
        profiles=profiles,
        planner_meta=planner_meta,
    )
