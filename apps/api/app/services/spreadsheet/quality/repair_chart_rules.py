from __future__ import annotations

from typing import Any

from ..core.schema import ChartSpec
from ..pipeline.column_profile import get_column_profiles
from .repair_common import best_column


def repair_chart_spec(df: Any, spec: ChartSpec, *, question: str) -> tuple[ChartSpec, dict[str, Any]]:
    profiles = get_column_profiles(df)
    columns = [str(column) for column in getattr(df, "columns", [])]
    data = spec.model_dump()
    changes: list[dict[str, Any]] = []

    x_resolved = best_column(str(data.get("x") or ""), columns, profiles)
    y_resolved = best_column(str(data.get("y") or ""), columns, profiles)

    numeric_columns = [column for column, profile in profiles.items() if str(profile.get("semantic_type") or "") == "numeric"]
    fallback_x = columns[0] if columns else "label"
    fallback_y = numeric_columns[0] if numeric_columns else (columns[1] if len(columns) > 1 else fallback_x)

    if not x_resolved:
        changes.append({"field": "x", "from": data.get("x"), "to": fallback_x, "reason": "fallback_chart_x"})
        data["x"] = fallback_x
    elif x_resolved != data.get("x"):
        changes.append({"field": "x", "from": data.get("x"), "to": x_resolved, "reason": "resolved_chart_x"})
        data["x"] = x_resolved

    if not y_resolved:
        changes.append({"field": "y", "from": data.get("y"), "to": fallback_y, "reason": "fallback_chart_y"})
        data["y"] = fallback_y
    elif y_resolved != data.get("y"):
        changes.append({"field": "y", "from": data.get("y"), "to": y_resolved, "reason": "resolved_chart_y"})
        data["y"] = y_resolved

    if not data.get("title"):
        data["title"] = str(question or "").strip() or "Chart"
        changes.append({"field": "title", "from": None, "to": data["title"], "reason": "default_title"})

    return ChartSpec.model_validate(data), {"changes": changes}
