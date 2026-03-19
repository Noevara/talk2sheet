from __future__ import annotations

from typing import Any

from ..core.schema import Metric, SelectionPlan, TransformPlan


SUPPORTED_FILTER_OPS = {"=", "!=", ">", ">=", "<", "<=", "in", "contains"}
SUPPORTED_METRIC_OPS = {"count_rows", "count_distinct", "nunique", "sum", "avg", "min", "max"}
SUPPORTED_DATE_GRAINS = {"day", "week", "month", "quarter", "weekday", "weekpart"}
SUPPORTED_BINARY_OPS = {"add", "sub", "mul", "div"}


def exact_execution_support(selection_plan: SelectionPlan, transform_plan: TransformPlan) -> dict[str, Any]:
    reasons: list[str] = []

    for flt in selection_plan.filters or []:
        if flt.op not in SUPPORTED_FILTER_OPS:
            reasons.append(f"filter.op:{flt.op}")

    for derived in transform_plan.derived_columns or []:
        if derived.kind == "date_bucket":
            if not derived.source_col:
                reasons.append("derived.source_col")
            if not derived.grain or derived.grain not in SUPPORTED_DATE_GRAINS:
                reasons.append(f"derived.grain:{derived.grain}")
            continue
        if derived.kind == "arithmetic":
            if not derived.left or not derived.right:
                reasons.append("derived.operands")
            if not derived.op or derived.op not in SUPPORTED_BINARY_OPS:
                reasons.append(f"derived.op:{derived.op}")
            continue
        reasons.append(f"derived.kind:{derived.kind}")

    for formula in transform_plan.formula_metrics or []:
        if formula.op not in SUPPORTED_BINARY_OPS:
            reasons.append(f"formula.op:{formula.op}")
        if not formula.left or not formula.right:
            reasons.append("formula.operands")

    for formula in transform_plan.post_pivot_formula_metrics or []:
        if formula.op not in SUPPORTED_BINARY_OPS:
            reasons.append(f"post_pivot_formula.op:{formula.op}")
        if not formula.left or not formula.right:
            reasons.append("post_pivot_formula.operands")

    for flt in (transform_plan.having or []) + (transform_plan.post_pivot_having or []):
        if flt.op not in SUPPORTED_FILTER_OPS:
            reasons.append(f"having.op:{flt.op}")

    if transform_plan.post_pivot_formula_metrics and transform_plan.pivot is None:
        reasons.append("post_pivot_formula_metrics.requires_pivot")
    if transform_plan.post_pivot_having and transform_plan.pivot is None:
        reasons.append("post_pivot_having.requires_pivot")

    metrics = [] if transform_plan.return_rows else (transform_plan.metrics or [Metric(agg="count_rows", col=None, as_name="count")])
    for metric in metrics:
        if metric.agg not in SUPPORTED_METRIC_OPS:
            reasons.append(f"metric.agg:{metric.agg}")

    return {"eligible": not reasons, "reasons": reasons}
