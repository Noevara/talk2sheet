from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, Protocol

from pydantic import BaseModel, Field

from app.config import get_settings
from ..openai_compatible import OpenAICompatibleError, build_default_llm_client
from ..planning.planning_prompts import build_followup_interpreter_prompt


class FollowupValueFilter(BaseModel):
    column_hint: str | None = None
    value: str


class FollowupInterpretation(BaseModel):
    kind: Literal[
        "new_question",
        "followup_refine",
        "followup_lookup",
        "followup_explain",
        "followup_filter",
        "followup_switch",
    ] = "new_question"
    standalone_question: str = ""
    requires_previous_context: bool = False
    preserve_previous_analysis: bool = False
    output_mode: Literal["auto", "text", "chart"] = "auto"
    target_rank: int | None = None
    target_label: str | None = None
    new_dimension: str | None = None
    view_intent: Literal["auto", "ranking", "share", "trend", "detail_rows", "comparison", "text_answer"] = "auto"
    compare_basis: Literal["auto", "previous_period", "year_over_year"] = "auto"
    time_grain: Literal["day", "week", "month", "quarter", "weekday", "weekpart"] | None = None
    time_value: str | None = None
    time_operator: Literal["=", "contains"] | None = None
    top_k: int | None = None
    value_filters: list[FollowupValueFilter] = Field(default_factory=list)
    confidence: float = 0.0
    reasoning: str = ""


@dataclass
class InterpretationResult:
    interpretation: FollowupInterpretation | None
    meta: dict[str, Any]


class SpreadsheetContextInterpreter(Protocol):
    name: str

    def interpret(
        self,
        df: Any,
        *,
        chat_text: str,
        requested_mode: str,
        followup_context: dict[str, Any] | None = None,
    ) -> InterpretationResult:
        ...


class NoopContextInterpreter:
    name = "noop"

    def interpret(
        self,
        df: Any,
        *,
        chat_text: str,
        requested_mode: str,
        followup_context: dict[str, Any] | None = None,
    ) -> InterpretationResult:
        return InterpretationResult(
            interpretation=None,
            meta={"provider": self.name, "used": False, "reason": "interpreter_disabled"},
        )


class LLMContextInterpreter:
    name = "openai-followup-v1"

    def __init__(self, client: Any | None = None) -> None:
        self.client = client if client is not None else build_default_llm_client()

    def interpret(
        self,
        df: Any,
        *,
        chat_text: str,
        requested_mode: str,
        followup_context: dict[str, Any] | None = None,
    ) -> InterpretationResult:
        if not isinstance(followup_context, dict) or not followup_context:
            return InterpretationResult(
                interpretation=None,
                meta={"provider": self.name, "used": False, "reason": "no_followup_context"},
            )
        if not getattr(self.client, "enabled", False):
            return InterpretationResult(
                interpretation=None,
                meta={"provider": self.name, "used": False, "reason": "llm_not_configured"},
            )

        system_prompt, user_prompt = build_followup_interpreter_prompt(
            df,
            question=chat_text,
            requested_mode=requested_mode,
            followup_context=followup_context,
        )
        payload = self.client.generate_json(
            FollowupInterpretation,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
        )
        return InterpretationResult(
            interpretation=payload,
            meta={
                "provider": self.name,
                "used": True,
                "confidence": float(payload.confidence or 0.0),
                "kind": payload.kind,
                "preserve_previous_analysis": bool(payload.preserve_previous_analysis),
            },
        )


def get_default_context_interpreter() -> SpreadsheetContextInterpreter:
    settings = get_settings()
    provider = str(settings.context_interpreter_provider or "auto").strip().lower()
    if provider in {"", "off", "disabled", "none", "heuristic"}:
        return NoopContextInterpreter()
    if provider in {"auto", "openai", "ark"}:
        interpreter = LLMContextInterpreter()
        if getattr(interpreter.client, "enabled", False):
            return interpreter
    return NoopContextInterpreter()
