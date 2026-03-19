from __future__ import annotations

from app.config import get_settings
from ..conversation.context_interpreter import get_default_context_interpreter
from ..openai_compatible import build_default_llm_client
from .followup.planner_followup_context import infer_mode
from .planner_heuristic import HeuristicPlanner as BaseHeuristicPlanner
from .planner_llm import FallbackPlanner, OpenAIJsonPlannerImpl
from .planner_types import PlanDraft, SpreadsheetPlanner


class HeuristicPlanner(BaseHeuristicPlanner):
    def __init__(self) -> None:
        super().__init__(context_interpreter_factory=get_default_context_interpreter)


class OpenAIJsonPlanner(OpenAIJsonPlannerImpl):
    def __init__(self) -> None:
        super().__init__(
            client_factory=build_default_llm_client,
            context_interpreter_factory=get_default_context_interpreter,
        )


def get_default_planner() -> SpreadsheetPlanner:
    settings = get_settings()
    heuristic = HeuristicPlanner()
    provider = str(settings.planner_provider or "auto").strip().lower()
    if provider == "heuristic":
        return heuristic
    if provider in {"openai", "auto", "ark"}:
        llm = OpenAIJsonPlanner()
        if llm.client.enabled:
            return FallbackPlanner(llm, heuristic)
    return heuristic
