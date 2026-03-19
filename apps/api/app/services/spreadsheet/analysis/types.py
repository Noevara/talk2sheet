from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd
from app.schemas import ExecutionDisclosure
from ..core.schema import ChartSpec, SelectionPlan, TransformPlan


@dataclass
class AnalysisPayload:
    mode: str
    answer: str
    pipeline: dict[str, Any]
    execution_disclosure: ExecutionDisclosure
    chart_spec: dict[str, Any] | None = None
    chart_data: list[dict[str, Any]] | None = None
    analysis_text: str | None = None


@dataclass
class SelectionStageResult:
    selection_plan: SelectionPlan
    selection_guardrail: dict[str, Any]
    selection_issues: list[dict[str, Any]]
    selection_repair: dict[str, Any] | None
    clarification: Any | None
    selected_df: pd.DataFrame | None = None
    selection_meta: dict[str, Any] | None = None


@dataclass
class TransformStageResult:
    transform_plan: TransformPlan
    transform_guardrail: dict[str, Any]
    transform_issues: list[dict[str, Any]]
    transform_repair: dict[str, Any] | None


@dataclass
class ExecutionStageResult:
    result_df: pd.DataFrame
    selection_meta: dict[str, Any]
    transform_meta: dict[str, Any]
    exact_used: bool
    exact_support: dict[str, Any]
    exact_source: dict[str, Any]


@dataclass
class ChartStageResult:
    chart_spec: ChartSpec | None
    chart_guardrail_meta: dict[str, Any]
    chart_validation: list[dict[str, Any]]
    chart_repair_meta: dict[str, Any]
    chart_data: list[dict[str, Any]] | None
