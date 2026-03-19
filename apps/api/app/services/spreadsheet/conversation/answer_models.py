from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field

from app.schemas import ExecutionDisclosure
from ..core.schema import ChartSpec, SelectionPlan, TransformPlan
from ..planning.planner_types import PlanDraft


@dataclass
class AnswerGeneratorContext:
    locale: str
    draft: PlanDraft
    result_df: pd.DataFrame
    selection_plan: SelectionPlan
    transform_plan: TransformPlan
    selection_meta: dict[str, Any] = field(default_factory=dict)
    transform_meta: dict[str, Any] = field(default_factory=dict)
    chart_spec: ChartSpec | None = None
    followup_context: dict[str, Any] | None = None
    execution_disclosure: ExecutionDisclosure | None = None
    chat_text: str = ""


@dataclass
class GeneratedAnswer:
    answer: str
    analysis_text: str
    meta: dict[str, Any] = field(default_factory=dict)
    segments: dict[str, str] = field(default_factory=dict)


@dataclass
class AnswerGeneratorBinding:
    generator: "SpreadsheetAnswerGenerator"
    requested_provider: str
    fallback_reason: str = ""


class SpreadsheetAnswerGenerator(Protocol):
    name: str

    def generate(self, context: AnswerGeneratorContext) -> GeneratedAnswer:
        ...


class LLMGeneratedAnswerModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    conclusion: str = Field(min_length=1, max_length=240)
    evidence: str = Field(min_length=1, max_length=900)
    risk_note: str = Field(default="", max_length=320)
    key_points: list[str] = Field(default_factory=list, max_length=5)
