from __future__ import annotations

import re
from typing import Any

from ..execution.executor import resolve_column_reference


_NUMERIC_LITERAL_RE = re.compile(r"-?\d+(?:\.\d+)?")


def best_column(requested: str, columns: list[str], profiles: dict[str, dict[str, Any]]) -> str | None:
    resolved = resolve_column_reference(requested, columns, profiles=profiles)
    candidate = str(resolved.get("resolved") or "")
    if not candidate or resolved.get("confidence") == "low":
        return None
    return candidate


def is_numeric_literal(value: Any) -> bool:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return True
    return _NUMERIC_LITERAL_RE.fullmatch(str(value or "").strip().replace(",", "")) is not None


def repair_operand(
    operand: Any,
    *,
    columns: list[str],
    profiles: dict[str, dict[str, Any]],
) -> tuple[Any | None, str | None]:
    if operand is None or str(operand).strip() == "":
        return None, "missing_operand"
    if is_numeric_literal(operand):
        return operand, None
    resolved = best_column(str(operand), columns, profiles)
    if resolved:
        return resolved, ("resolved_operand" if resolved != operand else None)
    return None, "unresolved_operand"
