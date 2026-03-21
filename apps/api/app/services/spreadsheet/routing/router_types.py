from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

from ..core.schema import Clarification


class SheetRoutingDecision(BaseModel):
    status: Literal["resolved", "clarification", "unsupported"] = "resolved"
    requested_sheet_index: int = 1
    resolved_sheet_index: int | None = None
    resolved_sheet_name: str = ""
    reason: str = ""
    matched_by: str = ""
    confidence: float = 0.0
    boundary_status: Literal["single_sheet_in_scope", "multi_sheet_detected", "multi_sheet_out_of_scope"] = "single_sheet_in_scope"
    boundary_reason: str = ""
    decomposition_hint: str = ""
    mentioned_sheets: list[dict[str, Any]] = Field(default_factory=list)
    explanation_code: str = ""
    explanation: str = ""
    clarification: Clarification | None = None
    candidate_scores: list[dict[str, Any]] = Field(default_factory=list)
