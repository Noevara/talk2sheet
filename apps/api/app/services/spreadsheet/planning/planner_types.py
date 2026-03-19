from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Protocol

from ..core.schema import ChartSpec, SelectionPlan, TransformPlan
from .intent_models import AnalysisIntent


@dataclass
class PlanDraft:
    mode: str
    intent: str
    selection_plan: SelectionPlan
    transform_plan: TransformPlan
    chart_spec: ChartSpec | None = None
    analysis_intent: AnalysisIntent | None = None
    planner_meta: dict[str, Any] = field(default_factory=dict)


@dataclass
class ResolvedColumns:
    amount_column: str | None
    date_column: str | None
    category_column: str | None
    single_transaction_column: str | None
    service_column: str | None
    region_column: str | None
    item_preferred_column: str | None
    item_column: str | None
    raw_question_dimension_column: str | None
    question_dimension_column: str | None


@dataclass
class FollowupPlanningState:
    followup_context: dict[str, Any] | None
    context_interpreter_meta: dict[str, Any]
    effective_chat_text: str
    mode: str
    preserve_previous_analysis: bool


@dataclass
class HeuristicPlanningContext:
    followup: FollowupPlanningState
    columns: ResolvedColumns
    profiles: dict[str, dict[str, Any]]
    planner_meta: dict[str, Any]


@dataclass
class HeuristicActionRuntimeContext:
    effective_chat_text: str
    mode: str
    followup_context: dict[str, Any] | None
    preserve_previous_analysis: bool
    profiles: dict[str, dict[str, Any]]
    planner_meta: dict[str, Any]
    amount_column: str | None
    date_column: str | None
    category_column: str | None
    single_transaction_column: str | None
    item_preferred_column: str | None
    item_column: str | None
    question_dimension_column: str | None


@dataclass
class ReuseFollowupRuntimeContext:
    mode: str
    amount_column: str | None
    date_column: str | None
    raw_question_dimension_column: str | None
    question_dimension_column: str | None
    service_column: str | None
    region_column: str | None
    item_column: str | None
    item_preferred_column: str | None
    category_column: str | None
    followup_context: dict[str, Any] | None
    planner_meta: dict[str, Any]
    profiles: dict[str, dict[str, Any]]

    @property
    def candidate_columns(self) -> list[str | None]:
        return [
            self.raw_question_dimension_column,
            self.question_dimension_column,
            self.service_column,
            self.region_column,
            self.item_column,
            self.item_preferred_column,
            self.category_column,
        ]


@dataclass
class FollowupSignalResolvers:
    rank_position_from_text: Callable[[str], int | None]
    top_k_followup: Callable[[str, dict[str, Any] | None], int | None]
    mode_switch_followup: Callable[..., bool]
    rank_lookup_followup: Callable[[str, dict[str, Any] | None], int | None]
    share_switch_followup: Callable[[str, dict[str, Any] | None], bool]
    dimension_switch_followup: Callable[..., bool]
    trend_switch_followup: Callable[[str, dict[str, Any] | None], bool]
    detail_switch_followup: Callable[[str, dict[str, Any] | None], bool]
    time_filter_followup: Callable[[str, dict[str, Any] | None], tuple[str, str] | None]


class SpreadsheetPlanner(Protocol):
    name: str

    def plan(self, df: Any, *, chat_text: str, requested_mode: str, followup_context: dict[str, Any] | None = None) -> PlanDraft:
        ...
