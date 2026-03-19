from __future__ import annotations

from typing import Any, Callable

from ..conversation.context_interpreter import get_default_context_interpreter
from ..pipeline.column_profile import get_column_profiles
from ..execution.executor import apply_selection, apply_transform
from ..openai_compatible import OpenAICompatibleError, build_default_llm_client
from .intent_understanding import understand_analysis_intent
from .planner_intent_signals import (
    _average_amount_question,
    _compare_question,
    _distinct_question,
    _extract_chart_type,
    _ranking_question,
    _row_count_question,
    _share_question,
    _total_amount_question,
    _trend_question,
    _weekday_weekend_question,
)
from .planner_text_utils import _extract_top_k
from .planner_types import PlanDraft, SpreadsheetPlanner
from .planner_rules import build_heuristic_planning_context
from .planning_prompts import build_chart_prompt, build_selection_prompt, build_transform_prompt
from ..core.schema import ChartSpec, SelectionPlan, TransformPlan
from ..quality.policy import PLANNER_LIGHT_POLICY, build_governance_meta, govern_plan, has_error
from ..quality.validator import summarize_issues


def _prepare_stage_plan(df: Any, plan: Any, *, question: str, mode: str) -> tuple[Any, dict[str, Any]]:
    governance = govern_plan(
        df,
        plan,
        question=question,
        mode=mode,
        policy=PLANNER_LIGHT_POLICY,
    )
    if has_error(governance.issues):
        raise OpenAICompatibleError(summarize_issues(governance.issues))
    return governance.plan, build_governance_meta(governance)


def _default_chart_spec(result_df: Any, *, question: str, top_k: int | None = None) -> ChartSpec:
    profiles = get_column_profiles(result_df)
    columns = [str(column) for column in getattr(result_df, "columns", [])]
    if not columns:
        return ChartSpec(type="bar", title=question.strip() or "Chart", x="label", y="value", top_k=top_k or 10)

    numeric_columns = [column for column in columns if str((profiles.get(column) or {}).get("semantic_type") or "") == "numeric"]
    date_columns = [column for column in columns if str((profiles.get(column) or {}).get("semantic_type") or "") == "date"]
    category_columns = [column for column in columns if column not in numeric_columns]

    x = date_columns[0] if date_columns else (category_columns[0] if category_columns else columns[0])
    y = numeric_columns[0] if numeric_columns else (columns[1] if len(columns) > 1 else columns[0])

    default_type = "line" if x in date_columns else "bar"
    chart_type = _extract_chart_type(question, default=default_type)
    if chart_type == "pie" and len(result_df.index) > 12:
        chart_type = "bar"

    return ChartSpec(
        type=chart_type,
        title=question.strip() or "Chart",
        x=x,
        y=y,
        top_k=top_k if top_k is not None else _extract_top_k(question, 10),
    )


def _infer_intent(chat_text: str, transform_plan: TransformPlan, chart_spec: ChartSpec | None) -> str:
    if transform_plan.return_rows:
        return "detail_rows"

    if transform_plan.pivot is not None and any(str(item.as_name) == "change_value" for item in (transform_plan.post_pivot_formula_metrics or [])):
        return "period_compare"

    metrics = transform_plan.metrics or []
    if (
        len(metrics) == 1
        and not transform_plan.groupby
        and not transform_plan.derived_columns
        and not transform_plan.formula_metrics
        and transform_plan.pivot is None
    ):
        agg = metrics[0].agg
        if agg == "count_rows":
            return "row_count"
        if agg in {"count_distinct", "nunique"}:
            return "count_distinct"
        if agg == "sum":
            return "total_amount"
        if agg == "avg":
            return "average_amount"

    if chart_spec is not None and chart_spec.type == "pie":
        return "share"
    if _share_question(chat_text):
        return "share"
    if _weekday_weekend_question(chat_text):
        return "weekpart_compare"
    if _compare_question(chat_text):
        return "period_compare"
    if _trend_question(chat_text) or any(item.kind == "date_bucket" for item in transform_plan.derived_columns):
        month_bucket_alias = next(
            (str(item.as_name or "") for item in transform_plan.derived_columns if item.kind == "date_bucket" and item.grain == "month"),
            "",
        )
        if month_bucket_alias and transform_plan.groupby == [month_bucket_alias] and not _trend_question(chat_text):
            return "period_breakdown"
        return "trend"
    if transform_plan.groupby or chart_spec is not None or _ranking_question(chat_text):
        return "ranking"
    if _row_count_question(chat_text):
        return "row_count"
    if _distinct_question(chat_text):
        return "count_distinct"
    if _total_amount_question(chat_text):
        return "total_amount"
    if _average_amount_question(chat_text):
        return "average_amount"
    return "unsupported"


class OpenAIJsonPlannerImpl:
    name = "openai-json-v2"

    def __init__(
        self,
        *,
        client_factory: Callable[[], Any] = build_default_llm_client,
        context_interpreter_factory: Callable[[], Any] = get_default_context_interpreter,
    ) -> None:
        self.client_factory = client_factory
        self.context_interpreter_factory = context_interpreter_factory
        self.client = self.client_factory()

    def plan(self, df: Any, *, chat_text: str, requested_mode: str, followup_context: dict[str, Any] | None = None) -> PlanDraft:
        if not self.client.enabled:
            raise OpenAICompatibleError("LLM planner is not configured.")

        intent_context = build_heuristic_planning_context(
            df,
            chat_text=chat_text,
            requested_mode=requested_mode,
            followup_context=followup_context,
            context_interpreter_factory=self.context_interpreter_factory,
        )
        followup_context = intent_context.followup.followup_context
        context_interpreter_meta = intent_context.followup.context_interpreter_meta
        effective_chat_text = intent_context.followup.effective_chat_text
        mode = intent_context.followup.mode
        base_intent = understand_analysis_intent(
            question=effective_chat_text,
            mode=mode,
            profiles=intent_context.profiles,
            columns=intent_context.columns,
            followup_context=followup_context,
        )
        if base_intent.clarification is not None:
            return PlanDraft(
                mode="text",
                intent=base_intent.kind,
                selection_plan=SelectionPlan(),
                transform_plan=TransformPlan(),
                analysis_intent=base_intent,
                planner_meta={
                    "planner": self.name,
                    "llm_planner": True,
                    "prompt_style": "staged-v1",
                    "context_interpreter": context_interpreter_meta,
                    "intent_clarification": True,
                },
            )

        planner_meta: dict[str, Any] = {
            "planner": self.name,
            "llm_planner": True,
            "prompt_style": "staged-v1",
            "context_interpreter": context_interpreter_meta,
        }
        if followup_context:
            planner_meta["followup_context_used"] = True
            planner_meta["followup_turn_count"] = int(followup_context.get("turn_count") or 0)

        selection_system_prompt, selection_user_prompt = build_selection_prompt(
            df,
            question=effective_chat_text,
            requested_mode=requested_mode,
            followup_context=followup_context,
        )
        raw_selection_plan = self.client.generate_json(
            SelectionPlan,
            system_prompt=selection_system_prompt,
            user_prompt=selection_user_prompt,
        )
        selection_plan, selection_stage_meta = _prepare_stage_plan(
            df,
            raw_selection_plan,
            question=chat_text,
            mode=mode,
        )
        planner_meta["selection_stage"] = selection_stage_meta
        selected_df, selection_meta = apply_selection(df, selection_plan)
        planner_meta["selection_stage"]["selection_meta"] = selection_meta

        transform_system_prompt, transform_user_prompt = build_transform_prompt(
            selected_df,
            question=effective_chat_text,
            mode=mode,
            followup_context=followup_context,
        )
        raw_transform_plan = self.client.generate_json(
            TransformPlan,
            system_prompt=transform_system_prompt,
            user_prompt=transform_user_prompt,
        )
        transform_plan, transform_stage_meta = _prepare_stage_plan(
            selected_df,
            raw_transform_plan,
            question=chat_text,
            mode=mode,
        )
        planner_meta["transform_stage"] = transform_stage_meta

        should_prepare_chart = mode == "chart" or _trend_question(effective_chat_text) or _share_question(effective_chat_text) or _ranking_question(effective_chat_text)
        chart_spec: ChartSpec | None = None

        if should_prepare_chart:
            result_df, transform_meta = apply_transform(selected_df, transform_plan)
            planner_meta["transform_stage"]["transform_meta"] = transform_meta
            if mode == "chart":
                try:
                    chart_system_prompt, chart_user_prompt = build_chart_prompt(
                        result_df,
                        question=effective_chat_text,
                        followup_context=followup_context,
                    )
                    chart_spec = self.client.generate_json(
                        ChartSpec,
                        system_prompt=chart_system_prompt,
                        user_prompt=chart_user_prompt,
                    )
                    planner_meta["chart_stage"] = {"provider": "llm", "rows": int(len(result_df.index))}
                except Exception as exc:
                    chart_spec = _default_chart_spec(result_df, question=chat_text, top_k=transform_plan.top_k)
                    planner_meta["chart_stage"] = {
                        "provider": "heuristic-default",
                        "fallback_reason": str(exc),
                        "rows": int(len(result_df.index)),
                    }
            else:
                chart_spec = _default_chart_spec(result_df, question=chat_text, top_k=transform_plan.top_k)
                planner_meta["chart_stage"] = {"provider": "heuristic-default", "mode": "text", "rows": int(len(result_df.index))}

        intent = _infer_intent(effective_chat_text, transform_plan, chart_spec)
        return PlanDraft(
            mode=mode,
            intent=intent,
            selection_plan=selection_plan,
            transform_plan=transform_plan,
            chart_spec=chart_spec,
            analysis_intent=understand_analysis_intent(
                question=effective_chat_text,
                mode=mode,
                profiles=intent_context.profiles,
                columns=intent_context.columns,
                followup_context=followup_context,
                draft=PlanDraft(
                    mode=mode,
                    intent=intent,
                    selection_plan=selection_plan,
                    transform_plan=transform_plan,
                    chart_spec=chart_spec,
                    planner_meta=planner_meta,
                ),
            ),
            planner_meta=planner_meta,
        )


class FallbackPlanner:
    def __init__(self, primary: SpreadsheetPlanner, fallback: SpreadsheetPlanner) -> None:
        self.primary = primary
        self.fallback = fallback
        self.name = getattr(primary, "name", "primary")

    def plan(self, df: Any, *, chat_text: str, requested_mode: str, followup_context: dict[str, Any] | None = None) -> PlanDraft:
        try:
            draft = self.primary.plan(df, chat_text=chat_text, requested_mode=requested_mode, followup_context=followup_context)
            draft.planner_meta.setdefault("planner_provider", getattr(self.primary, "name", "primary"))
            return draft
        except Exception as exc:
            draft = self.fallback.plan(df, chat_text=chat_text, requested_mode=requested_mode, followup_context=followup_context)
            draft.planner_meta["planner_provider_requested"] = getattr(self.primary, "name", "primary")
            draft.planner_meta["planner_provider_used"] = getattr(self.fallback, "name", "fallback")
            draft.planner_meta["planner_fallback_reason"] = str(exc)
            return draft
