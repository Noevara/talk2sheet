from __future__ import annotations

from typing import Any

from ..core.numeric_coercion import coerce_float
from ..core.schema import FormulaMetric
from ..pipeline.column_profile import attach_column_profiles, get_column_profiles
from .column_resolution import resolve_column_reference
from .value_coercion import safe_numeric_series


def resolve_operand_series(df: Any, operand: str | None) -> tuple[Any, dict[str, Any]]:
    if operand is None:
        raise ValueError("Operand is required")
    literal = coerce_float(operand)
    if literal is not None:
        return literal, {"kind": "literal", "value": literal}
    profiles = get_column_profiles(df)
    resolved = resolve_column_reference(str(operand), list(df.columns), profiles=profiles)
    resolved_column = str(resolved.get("resolved") or "")
    if not resolved_column or resolved.get("confidence") == "low":
        raise ValueError(f"Unknown operand: {operand}")
    return df[resolved_column], {"kind": "column", "requested": operand, "resolved": resolved_column}


def _apply_binary_op(left_num: Any, right_num: Any, *, op: str) -> Any:
    if op == "add":
        return left_num + right_num
    if op == "sub":
        return left_num - right_num
    if op == "mul":
        return left_num * right_num
    if op == "div":
        if isinstance(right_num, (int, float)):
            return left_num / right_num if right_num not in (0, 0.0) else float("nan")
        return left_num / right_num.replace(0, float("nan"))
    raise ValueError(f"Unsupported formula op: {op}")


def apply_formula_metrics(df: Any, formula_metrics: list[FormulaMetric]) -> tuple[Any, list[dict[str, Any]]]:
    if not formula_metrics:
        return df, []
    out = df.copy()
    meta: list[dict[str, Any]] = []
    for formula in formula_metrics:
        left, left_meta = resolve_operand_series(out, formula.left)
        right, right_meta = resolve_operand_series(out, formula.right)
        left_num = safe_numeric_series(left)
        right_num = safe_numeric_series(right)
        out[formula.as_name] = _apply_binary_op(left_num, right_num, op=formula.op)
        meta.append({"as_name": formula.as_name, "op": formula.op, "left": left_meta, "right": right_meta})
    return attach_column_profiles(out), meta
