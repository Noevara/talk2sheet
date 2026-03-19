from __future__ import annotations

from typing import Any, Callable

from ..conversation.context_interpreter import get_default_context_interpreter
from .followup.planner_reuse import try_reuse_followup_plan
from .intent_understanding import understand_analysis_intent
from .planner_heuristic_actions import build_forecast_plan, try_build_heuristic_action
from .planner_rules import build_heuristic_planning_context
from .planner_runtime import (
    build_action_runtime_context,
    build_followup_signal_resolvers,
    build_reuse_runtime_context,
)
from .planner_types import PlanDraft
from ..core.schema import SelectionPlan, TransformPlan


class HeuristicPlanner:
    name = "heuristic-v1"

    def __init__(self, *, context_interpreter_factory: Callable[[], Any] = get_default_context_interpreter) -> None:
        self.context_interpreter_factory = context_interpreter_factory

    def plan(self, df: Any, *, chat_text: str, requested_mode: str, followup_context: dict[str, Any] | None = None) -> PlanDraft:
        context = build_heuristic_planning_context(
            df,
            chat_text=chat_text,
            requested_mode=requested_mode,
            followup_context=followup_context,
            context_interpreter_factory=self.context_interpreter_factory,
        )
        planner_meta = context.planner_meta
        reuse_runtime_context = build_reuse_runtime_context(context)
        action_runtime_context = build_action_runtime_context(context)
        signal_resolvers = build_followup_signal_resolvers()
        base_intent = understand_analysis_intent(
            question=context.followup.effective_chat_text,
            mode=context.followup.mode,
            profiles=context.profiles,
            columns=context.columns,
            followup_context=context.followup.followup_context,
        )

        if base_intent.clarification is not None:
            return PlanDraft(
                mode="text",
                intent=base_intent.kind,
                selection_plan=SelectionPlan(),
                transform_plan=TransformPlan(),
                analysis_intent=base_intent,
                planner_meta={**planner_meta, "intent_clarification": True},
            )

        forecast_plan = build_forecast_plan(
            df,
            chat_text=chat_text,
            date_column=context.columns.date_column,
            amount_column=context.columns.amount_column,
            planner_meta=planner_meta,
            plan_draft_factory=PlanDraft,
        )
        if forecast_plan is not None:
            forecast_plan.analysis_intent = understand_analysis_intent(
                question=context.followup.effective_chat_text,
                mode=forecast_plan.mode,
                profiles=context.profiles,
                columns=context.columns,
                followup_context=context.followup.followup_context,
                draft=forecast_plan,
            )
            return forecast_plan

        reused_followup = try_reuse_followup_plan(
            df,
            chat_text=chat_text,
            runtime_context=reuse_runtime_context,
            signal_resolvers=signal_resolvers,
            plan_draft_factory=PlanDraft,
        )
        if reused_followup is not None:
            reused_followup.analysis_intent = understand_analysis_intent(
                question=context.followup.effective_chat_text,
                mode=reused_followup.mode,
                profiles=context.profiles,
                columns=context.columns,
                followup_context=context.followup.followup_context,
                draft=reused_followup,
            )
            return reused_followup

        heuristic_action = try_build_heuristic_action(
            df,
            chat_text=chat_text,
            runtime_context=action_runtime_context,
            plan_draft_factory=PlanDraft,
        )
        if heuristic_action is not None:
            heuristic_action.analysis_intent = understand_analysis_intent(
                question=context.followup.effective_chat_text,
                mode=heuristic_action.mode,
                profiles=context.profiles,
                columns=context.columns,
                followup_context=context.followup.followup_context,
                draft=heuristic_action,
            )
            return heuristic_action

        return PlanDraft(
            mode="text",
            intent="unsupported",
            selection_plan=SelectionPlan(),
            transform_plan=TransformPlan(),
            analysis_intent=understand_analysis_intent(
                question=context.followup.effective_chat_text,
                mode="text",
                profiles=context.profiles,
                columns=context.columns,
                followup_context=context.followup.followup_context,
            ),
            planner_meta={**planner_meta, "fallback": True},
        )
