from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, Protocol, cast

from pydantic import BaseModel, Field

from app.config import get_settings
from ..openai_compatible import build_default_llm_client
from ..planning.planning_prompts import build_followup_interpreter_prompt


class FollowupValueFilter(BaseModel):
    column_hint: str | None = None
    value: str


class FollowupAnchorFilter(BaseModel):
    column: str
    op: str = "="
    value: str


class FollowupAnalysisAnchor(BaseModel):
    intent: str = ""
    metric_column: str | None = None
    metric_agg: str | None = None
    metric_alias: str | None = None
    dimension_column: str | None = None
    time_column: str | None = None
    time_grain: str | None = None
    filters: list[FollowupAnchorFilter] = Field(default_factory=list)


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
    switch_sheet: bool = False
    sheet_reference: Literal["auto", "current", "previous", "another"] = "auto"
    analysis_anchor: FollowupAnalysisAnchor | None = None
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


_SHEET_REFERENCE_ANOTHER_TOKENS = (
    "another sheet",
    "other sheet",
    "next sheet",
    "different sheet",
    "另一个sheet",
    "另外一个sheet",
    "另一个工作表",
    "另外一个工作表",
    "换个sheet",
    "换一个sheet",
    "再看另一个",
    "别的sheet",
    "其他sheet",
    "別のシート",
    "他のシート",
    "別シート",
)

_SHEET_REFERENCE_PREVIOUS_TOKENS = (
    "previous sheet",
    "last sheet",
    "back to previous sheet",
    "go back to previous sheet",
    "上一个sheet",
    "上一张sheet",
    "上一个工作表",
    "上一张工作表",
    "回到上一个sheet",
    "回到上一个工作表",
    "前一个sheet",
    "前一个工作表",
    "戻って前のシート",
    "前のシート",
)

_SHEET_REFERENCE_CURRENT_TOKENS = (
    "current sheet",
    "this sheet",
    "keep current sheet",
    "当前sheet",
    "这个sheet",
    "当前工作表",
    "这个工作表",
    "本sheet",
    "本工作表",
    "今のシート",
    "現在のシート",
)


def _contains_any(text: str, tokens: tuple[str, ...]) -> bool:
    lowered = str(text or "").strip().lower()
    return any(token in lowered for token in tokens)


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _safe_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _safe_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _anchor_filters_from_selection(selection_plan: dict[str, Any]) -> list[FollowupAnchorFilter]:
    filters: list[FollowupAnchorFilter] = []
    for item in _safe_list(selection_plan.get("filters")):
        payload = _safe_dict(item)
        column = _normalize_text(payload.get("col"))
        op = _normalize_text(payload.get("op")) or "="
        value = _normalize_text(payload.get("value"))
        if not column or not value:
            continue
        filters.append(FollowupAnchorFilter(column=column, op=op, value=value))
        if len(filters) >= 3:
            break
    return filters


def _anchor_from_context(followup_context: dict[str, Any] | None) -> FollowupAnalysisAnchor | None:
    if not isinstance(followup_context, dict):
        return None
    existing = followup_context.get("analysis_anchor")
    if isinstance(existing, dict):
        try:
            return FollowupAnalysisAnchor.model_validate(existing)
        except Exception:
            pass

    last_turn = _safe_dict(followup_context.get("last_turn"))
    summary = _safe_dict(followup_context.get("last_pipeline_summary"))
    selection_plan = _safe_dict(last_turn.get("selection_plan"))
    transform_plan = _safe_dict(last_turn.get("transform_plan"))

    metrics = _safe_list(transform_plan.get("metrics"))
    first_metric = _safe_dict(metrics[0]) if metrics else {}
    groupby = _safe_list(transform_plan.get("groupby"))
    derived_columns = _safe_list(transform_plan.get("derived_columns"))

    time_column = ""
    time_grain = ""
    for item in derived_columns:
        derived = _safe_dict(item)
        if _normalize_text(derived.get("kind")) != "date_bucket":
            continue
        time_column = _normalize_text(derived.get("source_col"))
        time_grain = _normalize_text(derived.get("grain"))
        if time_column or time_grain:
            break

    intent = _normalize_text(last_turn.get("intent")) or _normalize_text(summary.get("intent"))
    metric_column = _normalize_text(first_metric.get("col")) or _normalize_text(summary.get("target_metric"))
    metric_agg = _normalize_text(first_metric.get("agg"))
    metric_alias = _normalize_text(first_metric.get("as_name"))
    dimension_column = _normalize_text(groupby[0]) if groupby else _normalize_text(summary.get("target_dimension"))
    filters = _anchor_filters_from_selection(selection_plan)

    if not any([intent, metric_column, dimension_column, time_column, filters]):
        return None
    return FollowupAnalysisAnchor(
        intent=intent,
        metric_column=metric_column or None,
        metric_agg=metric_agg or None,
        metric_alias=metric_alias or None,
        dimension_column=dimension_column or None,
        time_column=time_column or None,
        time_grain=time_grain or None,
        filters=filters,
    )


def build_analysis_anchor_payload(
    *,
    followup_context: dict[str, Any] | None,
) -> dict[str, Any] | None:
    anchor = _anchor_from_context(followup_context)
    if anchor is None:
        return None
    return anchor.model_dump()


def _sheet_reference_hint(chat_text: str, followup_context: dict[str, Any] | None) -> Literal["auto", "current", "previous", "another"]:
    if isinstance(followup_context, dict):
        explicit_hint = str(followup_context.get("sheet_reference_hint") or "").strip().lower()
        if explicit_hint in {"current", "previous", "another"}:
            return cast(Literal["current", "previous", "another"], explicit_hint)
    text = str(chat_text or "").strip()
    if not text:
        return "auto"
    if _contains_any(text, _SHEET_REFERENCE_PREVIOUS_TOKENS):
        return "previous"
    if _contains_any(text, _SHEET_REFERENCE_ANOTHER_TOKENS):
        return "another"
    if _contains_any(text, _SHEET_REFERENCE_CURRENT_TOKENS):
        return "current"
    return "auto"


class HeuristicContextInterpreter:
    name = "heuristic-followup-v1"

    def interpret(
        self,
        df: Any,  # noqa: ARG002
        *,
        chat_text: str,
        requested_mode: str,  # noqa: ARG002
        followup_context: dict[str, Any] | None = None,
    ) -> InterpretationResult:
        if not isinstance(followup_context, dict) or not followup_context:
            return InterpretationResult(
                interpretation=None,
                meta={"provider": self.name, "used": False, "reason": "no_followup_context"},
            )
        if not bool(followup_context.get("is_followup")):
            return InterpretationResult(
                interpretation=None,
                meta={"provider": self.name, "used": False, "reason": "not_followup"},
            )

        anchor = _anchor_from_context(followup_context)
        sheet_reference = _sheet_reference_hint(chat_text, followup_context)
        if sheet_reference == "auto":
            return InterpretationResult(
                interpretation=None,
                meta={
                    "provider": self.name,
                    "used": False,
                    "reason": "no_sheet_reference",
                    "analysis_anchor_used": bool(anchor),
                },
            )

        switch_sheet = sheet_reference in {"another", "previous"}
        interpretation = FollowupInterpretation(
            kind="followup_switch",
            standalone_question=str(chat_text or "").strip(),
            requires_previous_context=True,
            preserve_previous_analysis=True,
            switch_sheet=switch_sheet,
            sheet_reference=sheet_reference,
            analysis_anchor=anchor,
            confidence=0.82,
            reasoning="Heuristically resolved sheet pronoun in follow-up context.",
        )
        return InterpretationResult(
            interpretation=interpretation,
            meta={
                "provider": self.name,
                "used": True,
                "confidence": float(interpretation.confidence or 0.0),
                "kind": interpretation.kind,
                "sheet_reference": sheet_reference,
                "analysis_anchor_used": bool(anchor),
            },
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
        if payload.analysis_anchor is None:
            fallback_anchor = _anchor_from_context(followup_context)
            if fallback_anchor is not None:
                payload = payload.model_copy(update={"analysis_anchor": fallback_anchor})
        return InterpretationResult(
            interpretation=payload,
            meta={
                "provider": self.name,
                "used": True,
                "confidence": float(payload.confidence or 0.0),
                "kind": payload.kind,
                "preserve_previous_analysis": bool(payload.preserve_previous_analysis),
                "analysis_anchor_used": payload.analysis_anchor is not None,
            },
        )


def get_default_context_interpreter() -> SpreadsheetContextInterpreter:
    settings = get_settings()
    provider = str(settings.context_interpreter_provider or "auto").strip().lower()
    if provider in {"", "off", "disabled", "none"}:
        return NoopContextInterpreter()
    if provider in {"heuristic"}:
        return HeuristicContextInterpreter()
    if provider in {"auto", "openai", "ark"}:
        interpreter = LLMContextInterpreter()
        if getattr(interpreter.client, "enabled", False):
            return interpreter
        return HeuristicContextInterpreter()
    return NoopContextInterpreter()
