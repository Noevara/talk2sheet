from __future__ import annotations

from typing import Any

import pandas as pd

from app.config import get_settings
from app.schemas import ExecutionDisclosure
from ..core.i18n import t
from ..pipeline.column_profile import get_column_profiles


def has_error(issues: list[dict[str, Any]]) -> bool:
    return any(str(item.get("severity") or "error") == "error" for item in issues)


def build_execution_disclosure(locale: str, *, rows_loaded: int, exact_used: bool, fallback_reason: str = "") -> ExecutionDisclosure:
    settings = get_settings()
    if exact_used:
        return ExecutionDisclosure(
            data_scope="exact_full_table",
            exact_used=True,
            scope_text=t(locale, "scope_exact"),
            max_rows=settings.max_analysis_rows,
        )
    if rows_loaded >= settings.max_analysis_rows:
        return ExecutionDisclosure(
            data_scope="sampled_first_n",
            exact_used=False,
            scope_text=t(locale, "scope_sampled", max_rows=settings.max_analysis_rows),
            scope_warning="",
            fallback_reason=fallback_reason or "The configured analysis row budget was reached.",
            fallback_reason_code="analysis_row_limit",
            max_rows=settings.max_analysis_rows,
        )
    return ExecutionDisclosure(
        data_scope="sampled_first_n",
        exact_used=False,
        scope_text=t(locale, "scope_sampled", max_rows=settings.max_analysis_rows),
        scope_warning="",
        fallback_reason=fallback_reason,
        fallback_reason_code="exact_execution_unavailable" if fallback_reason else "",
        max_rows=settings.max_analysis_rows,
    )


def fallback_pipeline(df: pd.DataFrame, *, reason: str, code: str) -> dict[str, Any]:
    return {
        "status": "blocked",
        "reason": reason,
        "reason_code": code,
        "column_profiles": list(get_column_profiles(df).values()),
    }
