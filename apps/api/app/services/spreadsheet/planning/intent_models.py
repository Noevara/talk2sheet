from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

from ..core.schema import Clarification


class AnalysisTimeScope(BaseModel):
    grain: str | None = None
    requested_period: str | None = None
    requested_periods: list[str] = Field(default_factory=list)
    base_period: str | None = None
    compare_window: list[str] = Field(default_factory=list)
    is_followup: bool = False


class AnalysisIntent(BaseModel):
    kind: str
    legacy_intent: str | None = None
    target_metric: str | None = None
    target_dimension: str | None = None
    comparison_type: str | None = None
    time_scope: AnalysisTimeScope | None = None
    answer_expectation: Literal["single_value", "summary_table", "detail_rows", "chart", "clarification", "unsupported"] | None = None
    clarification: Clarification | None = None
    join_requested: bool = False
    join_key: str | None = None
    join_type: str | None = None
    join_beta_eligible: bool | None = None
    join_gate_reasons: list[str] = Field(default_factory=list)
