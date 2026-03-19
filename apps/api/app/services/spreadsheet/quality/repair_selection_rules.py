from __future__ import annotations

from typing import Any

from ..core.schema import SelectionPlan
from ..pipeline.column_profile import get_column_profiles
from .repair_common import best_column


def repair_selection_plan(df: Any, plan: SelectionPlan, *, question: str, mode: str) -> tuple[SelectionPlan, dict[str, Any]]:
    profiles = get_column_profiles(df)
    columns = [str(column) for column in getattr(df, "columns", [])]
    data = plan.model_dump()
    changes: list[dict[str, Any]] = []

    repaired_columns: list[str] = []
    for column in data.get("columns") or []:
        resolved = best_column(column, columns, profiles)
        if resolved:
            repaired_columns.append(resolved)
            if resolved != column:
                changes.append({"field": "columns", "from": column, "to": resolved, "reason": "resolved_column"})
        else:
            changes.append({"field": "columns", "from": column, "to": None, "reason": "dropped_unresolved_column"})
    data["columns"] = list(dict.fromkeys(repaired_columns))

    repaired_filters = []
    for flt in data.get("filters") or []:
        resolved = best_column(str(flt.get("col") or ""), columns, profiles)
        if not resolved:
            changes.append({"field": "filters", "from": flt, "to": None, "reason": "dropped_unresolved_filter"})
            continue
        if resolved != flt.get("col"):
            changes.append({"field": "filters.col", "from": flt.get("col"), "to": resolved, "reason": "resolved_filter_column"})
        repaired_filters.append({**flt, "col": resolved})
    data["filters"] = repaired_filters

    distinct_by = data.get("distinct_by")
    if distinct_by:
        resolved = best_column(str(distinct_by), columns, profiles)
        if resolved:
            data["distinct_by"] = resolved
            if resolved != distinct_by:
                changes.append({"field": "distinct_by", "from": distinct_by, "to": resolved, "reason": "resolved_distinct_column"})
        else:
            changes.append({"field": "distinct_by", "from": distinct_by, "to": None, "reason": "dropped_unresolved_distinct"})
            data["distinct_by"] = None

    sort = data.get("sort")
    if sort:
        resolved = best_column(str(sort.get("col") or ""), columns, profiles)
        if resolved:
            data["sort"] = {**sort, "col": resolved}
            if resolved != sort.get("col"):
                changes.append({"field": "sort.col", "from": sort.get("col"), "to": resolved, "reason": "resolved_sort_column"})
        else:
            changes.append({"field": "sort", "from": sort, "to": None, "reason": "dropped_unresolved_sort"})
            data["sort"] = None

    return SelectionPlan.model_validate(data), {"changes": changes}
