from __future__ import annotations

from typing import Any

from ..core.schema import TransformPlan
from ..pipeline.column_profile import attach_column_profiles, get_column_profiles
from .column_resolution import pick_close_column
from .formula_ops import apply_formula_metrics
from .pivot_ops import apply_having, apply_pivot
from .value_coercion import sort_frame_by_column


def apply_post_agg_operations(df: Any, plan: TransformPlan) -> tuple[Any, dict[str, Any]]:
    meta: dict[str, Any] = {}
    out = attach_column_profiles(df.copy())

    out, formula_meta = apply_formula_metrics(out, plan.formula_metrics or [])
    if formula_meta:
        meta["formula_metrics"] = formula_meta

    out, having_meta = apply_having(out, plan.having or [])
    if having_meta:
        meta["having"] = having_meta

    out, pivot_meta = apply_pivot(out, plan.pivot)
    if pivot_meta:
        meta["pivot"] = pivot_meta

    if plan.post_pivot_formula_metrics and not plan.pivot:
        raise ValueError("post_pivot_formula_metrics requires pivot")
    if plan.post_pivot_having and not plan.pivot:
        raise ValueError("post_pivot_having requires pivot")

    out, post_formula_meta = apply_formula_metrics(out, plan.post_pivot_formula_metrics or [])
    if post_formula_meta:
        meta["post_pivot_formula_metrics"] = post_formula_meta

    out, post_having_meta = apply_having(out, plan.post_pivot_having or [])
    if post_having_meta:
        meta["post_pivot_having"] = post_having_meta

    if plan.order_by:
        profiles = get_column_profiles(out)
        column = pick_close_column(plan.order_by.col, list(out.columns), profiles=profiles)
        semantic_type = str((profiles.get(column) or {}).get("semantic_type") or "")
        out = sort_frame_by_column(out, column, plan.order_by.dir, semantic_type=semantic_type)
        meta["order_by"] = {"col": column, "dir": plan.order_by.dir}

    if plan.top_k is not None:
        out = out.head(int(plan.top_k))
        meta["top_k"] = int(plan.top_k)

    return attach_column_profiles(out), meta
