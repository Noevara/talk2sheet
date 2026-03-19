from __future__ import annotations

from typing import Any

from ..core.schema import ChartSpec
from ..execution.executor import resolve_column_reference
from ..pipeline.column_profile import get_column_profiles
from .validator_common import issue, validate_ref_with_scope


def validate_chart_spec(df: Any, spec: ChartSpec) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    columns = [str(column) for column in getattr(df, "columns", [])]
    profiles = get_column_profiles(df)

    issues.extend(validate_ref_with_scope(spec.x, field="chart.x", columns=columns, profiles=profiles))
    issues.extend(validate_ref_with_scope(spec.y, field="chart.y", columns=columns, profiles=profiles))

    resolved_y = resolve_column_reference(spec.y, columns, profiles=profiles)
    y_name = str(resolved_y.get("resolved") or "")
    y_type = str((profiles.get(y_name) or {}).get("semantic_type") or "")
    if y_name and y_type not in {"numeric", "unknown"}:
        issues.append(issue("chart_y_not_numeric", f"Chart y-axis should be numeric, got {y_name} ({y_type}).", field="chart.y"))

    if spec.type == "scatter":
        resolved_x = resolve_column_reference(spec.x, columns, profiles=profiles)
        x_name = str(resolved_x.get("resolved") or "")
        x_type = str((profiles.get(x_name) or {}).get("semantic_type") or "")
        if x_name and x_type not in {"numeric", "date", "unknown"}:
            issues.append(issue("chart_x_not_numeric", f"Scatter x-axis should be numeric or date-like, got {x_name} ({x_type}).", field="chart.x"))

    return issues
