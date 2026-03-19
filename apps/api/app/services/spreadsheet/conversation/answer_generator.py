from __future__ import annotations

from app.config import get_settings

from ..openai_compatible import OpenAICompatibleError, build_default_llm_client
from .answer_models import (
    AnswerGeneratorBinding,
    AnswerGeneratorContext,
    GeneratedAnswer,
    LLMGeneratedAnswerModel,
    SpreadsheetAnswerGenerator,
)
from .llm_based import LLMAnswerGenerator as _BaseLLMAnswerGenerator
from .rule_based import RuleBasedAnswerGenerator


class LLMAnswerGenerator(_BaseLLMAnswerGenerator):
    def __init__(
        self,
        *,
        provider_kind: str,
        fallback_generator: SpreadsheetAnswerGenerator | None = None,
        client: object | None = None,
    ) -> None:
        super().__init__(
            provider_kind=provider_kind,
            fallback_generator=fallback_generator,
            client=client,
            client_factory=build_default_llm_client,
        )


def get_default_answer_generator() -> AnswerGeneratorBinding:
    settings = get_settings()
    provider = str(settings.answer_provider or "rule").strip().lower()
    if provider in {"", "auto", "rule", "rule-v1"}:
        return AnswerGeneratorBinding(generator=RuleBasedAnswerGenerator(), requested_provider=provider or "rule")
    if provider in {"openai", "ark"}:
        return AnswerGeneratorBinding(
            generator=LLMAnswerGenerator(provider_kind=provider),
            requested_provider=provider,
        )
    return AnswerGeneratorBinding(
        generator=RuleBasedAnswerGenerator(),
        requested_provider=provider,
        fallback_reason="unknown_answer_provider",
    )


__all__ = [
    "AnswerGeneratorBinding",
    "AnswerGeneratorContext",
    "GeneratedAnswer",
    "LLMAnswerGenerator",
    "LLMGeneratedAnswerModel",
    "OpenAICompatibleError",
    "RuleBasedAnswerGenerator",
    "SpreadsheetAnswerGenerator",
    "build_default_llm_client",
    "get_default_answer_generator",
]
