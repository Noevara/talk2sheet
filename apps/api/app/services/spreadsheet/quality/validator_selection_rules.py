from __future__ import annotations

from typing import Any

from ..core.schema import SelectionPlan
from .validator_common import issue, validate_ref


def validate_selection_plan(df: Any, plan: SelectionPlan, *, question: str, mode: str) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    for index, column in enumerate(plan.columns or []):
        issues.extend(validate_ref(df, column, field=f"columns[{index}]"))
    for index, flt in enumerate(plan.filters or []):
        issues.extend(validate_ref(df, flt.col, field=f"filters[{index}].col"))
    if plan.distinct_by:
        issues.extend(validate_ref(df, plan.distinct_by, field="distinct_by"))
    if plan.sort:
        issues.extend(validate_ref(df, plan.sort.col, field="sort.col"))
    if mode == "chart" and plan.limit is not None:
        issues.append(issue("chart_limit", "Chart mode should not limit rows in SelectionPlan.", severity="warn", field="limit"))
    return issues
