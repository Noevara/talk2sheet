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
    clarification: Clarification | None = None
    candidate_scores: list[dict[str, Any]] = Field(default_factory=list)
