from __future__ import annotations

from typing import Any, Callable

from app.config import get_settings
from ..openai_compatible import OpenAICompatibleError, build_default_llm_client
from ..planning.intent_accessors import analysis_intent_kind, analysis_intent_payload
from .answer_models import AnswerGeneratorContext, AnswerGeneratorBinding, GeneratedAnswer, LLMGeneratedAnswerModel, SpreadsheetAnswerGenerator
from .formatters import (
    _finalize_generated_answer,
    _followup_summary,
    _locale_language,
    _normalize_segment,
    _preview_csv,
    _row_summary,
)
from .rule_based import RuleBasedAnswerGenerator


class LLMAnswerGenerator:
    def __init__(
        self,
        *,
        provider_kind: str,
        fallback_generator: SpreadsheetAnswerGenerator | None = None,
        client: Any | None = None,
        client_factory: Callable[[], Any] | None = None,
    ) -> None:
        self.provider_kind = str(provider_kind or "openai").strip().lower() or "openai"
        self.name = f"{self.provider_kind}-answer-v1"
        self.fallback_generator = fallback_generator or RuleBasedAnswerGenerator()
        factory = client_factory or build_default_llm_client
        self.client = client if client is not None else factory()

    def _build_prompts(self, context: AnswerGeneratorContext, baseline: GeneratedAnswer) -> tuple[str, str]:
        system_prompt = (
            "You are a spreadsheet analysis answer writer. "
            "You will receive already executed spreadsheet results and must only write the answer layer. "
            "Return JSON only and follow the schema exactly. "
            "Do not invent calculations, filters, trends, chart claims, or rows that are not grounded in the provided result. "
            "Write the response in three segments: `conclusion`, `evidence`, and `risk_note`. "
            "`conclusion` must be one concise sentence with the main finding only. "
            "`evidence` must be one or two concise sentences explaining the basis in the executed result. "
            "`risk_note` must be one concise sentence only when a caveat, sampled scope, fallback, or ambiguity matters; otherwise return an empty string. "
            "For ranking/trend/period_compare questions, keep the wording reusable for reporting: finding first, basis second, caveat optional. "
            "No markdown, no bullet points, no code fences. "
            "Use the requested locale language exactly. "
            "If no chart_spec is provided, do not mention a chart. "
            "If the result is empty, explicitly state that no matching rows were found. "
            "Preserve important numeric values and category labels from the provided result. "
            "Do not repeat the conclusion verbatim inside evidence."
        )
        preview_row_summary = _row_summary(context.result_df)
        baseline_segments = baseline.segments or {}
        user_prompt = (
            f"locale={context.locale}\n"
            f"language={_locale_language(context.locale)}\n"
            f"question={context.chat_text}\n"
            f"mode={context.draft.mode}\n"
            f"intent={analysis_intent_kind(context.draft, fallback=context.draft.intent)}\n"
            f"analysis_intent={analysis_intent_payload(context.draft)}\n"
            f"selection_plan={context.selection_plan.model_dump()}\n"
            f"transform_plan={context.transform_plan.model_dump()}\n"
            f"chart_spec={context.chart_spec.model_dump() if context.chart_spec is not None else None}\n"
            f"planner_meta={context.draft.planner_meta}\n"
            f"selection_meta={context.selection_meta}\n"
            f"transform_meta={context.transform_meta}\n"
            f"followup_context={_followup_summary(context.followup_context)}\n"
            f"execution_disclosure={context.execution_disclosure.model_dump() if context.execution_disclosure is not None else None}\n"
            f"result_row_count={int(len(context.result_df.index))}\n"
            f"result_columns={[str(column) for column in context.result_df.columns]}\n"
            f"result_preview_csv=\n{_preview_csv(context.result_df)}\n"
            f"result_first_row_summary={preview_row_summary}\n"
            f"reference_conclusion={baseline_segments.get('conclusion') or baseline.answer}\n"
            f"reference_evidence={baseline_segments.get('evidence') or baseline.analysis_text}\n"
            f"reference_risk_note={baseline_segments.get('risk_note') or ''}\n"
            "segment_policy=conclusion(1 sentence) | evidence(1-2 sentences) | risk_note(0-1 sentence)\n"
            f"rule_based_meta={baseline.meta}\n"
            "Write a user-facing answer grounded in the executed result."
        )
        return system_prompt, user_prompt

    def generate(self, context: AnswerGeneratorContext) -> GeneratedAnswer:
        baseline = self.fallback_generator.generate(context)
        if not getattr(self.client, "enabled", False):
            return GeneratedAnswer(
                answer=baseline.answer,
                analysis_text=baseline.analysis_text,
                meta={
                    **baseline.meta,
                    "provider_used": getattr(self.fallback_generator, "name", "rule-v1"),
                    "requested_generator": self.name,
                    "fallback_used": True,
                    "fallback_reason": "llm_answer_provider_not_configured",
                },
            )

        system_prompt, user_prompt = self._build_prompts(context, baseline)
        try:
            payload = self.client.generate_json(
                LLMGeneratedAnswerModel,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
            )
            baseline_segments = baseline.segments or {}
            conclusion = _normalize_segment(payload.conclusion)
            evidence = _normalize_segment(payload.evidence)
            if evidence == conclusion:
                evidence = _normalize_segment(baseline_segments.get("evidence") or baseline.analysis_text)
            risk_note = _normalize_segment(payload.risk_note) or baseline.segments.get("risk_note", "")
            if risk_note and risk_note in {conclusion, evidence}:
                risk_note = _normalize_segment(baseline.segments.get("risk_note", ""))
            if not conclusion or not evidence:
                raise OpenAICompatibleError("LLM answer payload was empty.")
            return _finalize_generated_answer(
                answer=conclusion,
                analysis_text=evidence,
                meta={
                    **baseline.meta,
                    "provider_used": self.name,
                    "requested_generator": self.name,
                    "fallback_used": False,
                    "llm_key_points": [str(item) for item in payload.key_points],
                    "llm_model": getattr(self.client, "model", ""),
                    "llm_base_url": bool(getattr(self.client, "base_url", "")),
                },
                conclusion=conclusion,
                evidence=evidence,
                risk_note=risk_note,
            )
        except Exception as exc:
            return GeneratedAnswer(
                answer=baseline.answer,
                analysis_text=baseline.analysis_text,
                meta={
                    **baseline.meta,
                    "provider_used": getattr(self.fallback_generator, "name", "rule-v1"),
                    "requested_generator": self.name,
                    "fallback_used": True,
                    "fallback_reason": str(exc),
                },
                segments=baseline.segments,
            )


def build_default_answer_generator_binding(
    *,
    provider: str,
    llm_generator_factory: Callable[[str], SpreadsheetAnswerGenerator],
) -> AnswerGeneratorBinding:
    if provider in {"", "auto", "rule", "rule-v1"}:
        return AnswerGeneratorBinding(generator=RuleBasedAnswerGenerator(), requested_provider=provider or "rule")
    if provider in {"openai", "ark"}:
        return AnswerGeneratorBinding(generator=llm_generator_factory(provider), requested_provider=provider)
    return AnswerGeneratorBinding(
        generator=RuleBasedAnswerGenerator(),
        requested_provider=provider,
        fallback_reason="unknown_answer_provider",
    )


def read_answer_provider() -> str:
    settings = get_settings()
    return str(settings.answer_provider or "rule").strip().lower()
