from __future__ import annotations

from typing import Any

from ..core.schema import Metric, TransformPlan
from ..pipeline.column_profile import get_column_profiles
from .repair_common import best_column, repair_operand
from .validator_transform_rules import infer_pivot_output_scope


def repair_transform_plan(df: Any, plan: TransformPlan, *, question: str, mode: str) -> tuple[TransformPlan, dict[str, Any]]:
    base_profiles = get_column_profiles(df)
    base_columns = [str(column) for column in getattr(df, "columns", [])]
    data = plan.model_dump()
    changes: list[dict[str, Any]] = []

    available_input_columns = list(base_columns)

    repaired_derived = []
    for item in data.get("derived_columns") or []:
        alias = str(item.get("as_name") or "").strip()
        if item.get("kind") == "date_bucket":
            resolved = best_column(str(item.get("source_col") or ""), available_input_columns, base_profiles)
            if not resolved:
                changes.append({"field": "derived_columns", "from": item, "to": None, "reason": "dropped_unresolved_source"})
                continue
            if resolved != item.get("source_col"):
                changes.append({"field": "derived_columns.source_col", "from": item.get("source_col"), "to": resolved, "reason": "resolved_source_column"})
            item["source_col"] = resolved
        elif item.get("kind") == "arithmetic":
            valid = True
            for field_name in ("left", "right"):
                operand = item.get(field_name)
                repaired_operand, reason = repair_operand(operand, columns=available_input_columns, profiles=base_profiles)
                if repaired_operand is None:
                    valid = False
                    break
                if reason == "resolved_operand" and repaired_operand != operand:
                    changes.append(
                        {
                            "field": f"derived_columns.{field_name}",
                            "from": operand,
                            "to": repaired_operand,
                            "reason": "resolved_operand",
                        }
                    )
                item[field_name] = repaired_operand
            if not valid:
                changes.append({"field": "derived_columns", "from": item, "to": None, "reason": "dropped_unresolved_arithmetic_operand"})
                continue
        repaired_derived.append(item)
        if alias:
            available_input_columns.append(alias)
    data["derived_columns"] = repaired_derived

    repaired_groupby = []
    for column in data.get("groupby") or []:
        resolved = best_column(str(column), available_input_columns, base_profiles)
        if resolved:
            repaired_groupby.append(resolved)
            if resolved != column:
                changes.append({"field": "groupby", "from": column, "to": resolved, "reason": "resolved_groupby_column"})
        else:
            changes.append({"field": "groupby", "from": column, "to": None, "reason": "dropped_unresolved_groupby"})
    data["groupby"] = repaired_groupby

    repaired_metrics = []
    pre_pivot_output_columns: list[str] = list(repaired_groupby)
    for metric in data.get("metrics") or []:
        agg = str(metric.get("agg") or "")
        if agg != "count_rows":
            resolved = best_column(str(metric.get("col") or ""), available_input_columns, base_profiles)
            if not resolved:
                changes.append({"field": "metrics", "from": metric, "to": None, "reason": "dropped_unresolved_metric_column"})
                continue
            if resolved != metric.get("col"):
                changes.append({"field": "metrics.col", "from": metric.get("col"), "to": resolved, "reason": "resolved_metric_column"})
            metric["col"] = resolved
        repaired_metrics.append(metric)
        alias = str(metric.get("as_name") or "").strip()
        if alias:
            pre_pivot_output_columns.append(alias)
    data["metrics"] = repaired_metrics

    if data.get("return_rows"):
        pre_pivot_output_columns = list(available_input_columns)

    repaired_formula_metrics = []
    for formula in data.get("formula_metrics") or []:
        valid = True
        for field_name in ("left", "right"):
            operand = formula.get(field_name)
            repaired_operand, reason = repair_operand(operand, columns=pre_pivot_output_columns, profiles=base_profiles)
            if repaired_operand is None:
                valid = False
                break
            if reason == "resolved_operand" and repaired_operand != operand:
                changes.append(
                    {
                        "field": f"formula_metrics.{field_name}",
                        "from": operand,
                        "to": repaired_operand,
                        "reason": "resolved_formula_operand",
                    }
                )
            formula[field_name] = repaired_operand
        if not valid:
            changes.append({"field": "formula_metrics", "from": formula, "to": None, "reason": "dropped_unresolved_formula_operand"})
            continue
        repaired_formula_metrics.append(formula)
        alias = str(formula.get("as_name") or "").strip()
        if alias:
            pre_pivot_output_columns.append(alias)
    data["formula_metrics"] = repaired_formula_metrics

    repaired_having = []
    for flt in data.get("having") or []:
        resolved = best_column(str(flt.get("col") or ""), pre_pivot_output_columns, base_profiles)
        if resolved:
            repaired_having.append({**flt, "col": resolved})
            if resolved != flt.get("col"):
                changes.append({"field": "having.col", "from": flt.get("col"), "to": resolved, "reason": "resolved_having_column"})
        else:
            changes.append({"field": "having", "from": flt, "to": None, "reason": "dropped_unresolved_having"})
    data["having"] = repaired_having

    repaired_pivot = data.get("pivot")
    if repaired_pivot:
        repaired_index: list[str] = []
        for column in repaired_pivot.get("index") or []:
            resolved = best_column(str(column), pre_pivot_output_columns, base_profiles)
            if resolved:
                repaired_index.append(resolved)
                if resolved != column:
                    changes.append({"field": "pivot.index", "from": column, "to": resolved, "reason": "resolved_pivot_index"})
            else:
                changes.append({"field": "pivot.index", "from": column, "to": None, "reason": "dropped_unresolved_pivot_index"})
        repaired_pivot["index"] = repaired_index

        pivot_columns_resolved = best_column(str(repaired_pivot.get("columns") or ""), pre_pivot_output_columns, base_profiles)
        pivot_values_resolved = best_column(str(repaired_pivot.get("values") or ""), pre_pivot_output_columns, base_profiles)
        if pivot_columns_resolved and pivot_values_resolved:
            if pivot_columns_resolved != repaired_pivot.get("columns"):
                changes.append({"field": "pivot.columns", "from": repaired_pivot.get("columns"), "to": pivot_columns_resolved, "reason": "resolved_pivot_columns"})
            if pivot_values_resolved != repaired_pivot.get("values"):
                changes.append({"field": "pivot.values", "from": repaired_pivot.get("values"), "to": pivot_values_resolved, "reason": "resolved_pivot_values"})
            repaired_pivot["columns"] = pivot_columns_resolved
            repaired_pivot["values"] = pivot_values_resolved
            data["pivot"] = repaired_pivot
        else:
            changes.append({"field": "pivot", "from": repaired_pivot, "to": None, "reason": "dropped_unresolved_pivot"})
            data["pivot"] = None

    post_pivot_columns = list(pre_pivot_output_columns)
    if data.get("pivot"):
        scope = infer_pivot_output_scope(df, TransformPlan.model_validate(data))
        if scope.get("inferred"):
            post_pivot_columns = [str(column) for column in (scope.get("final_columns") or [])]
        else:
            post_pivot_columns = [str(column) for column in (data["pivot"].get("index") or [])]

    if not data.get("pivot"):
        if data.get("post_pivot_formula_metrics"):
            changes.append(
                {
                    "field": "post_pivot_formula_metrics",
                    "from": data.get("post_pivot_formula_metrics"),
                    "to": [],
                    "reason": "requires_pivot",
                }
            )
            data["post_pivot_formula_metrics"] = []
        if data.get("post_pivot_having"):
            changes.append({"field": "post_pivot_having", "from": data.get("post_pivot_having"), "to": [], "reason": "requires_pivot"})
            data["post_pivot_having"] = []

    repaired_post_pivot_formula_metrics = []
    for formula in data.get("post_pivot_formula_metrics") or []:
        valid = True
        for field_name in ("left", "right"):
            operand = formula.get(field_name)
            repaired_operand, reason = repair_operand(operand, columns=post_pivot_columns, profiles=base_profiles)
            if repaired_operand is None:
                valid = False
                break
            if reason == "resolved_operand" and repaired_operand != operand:
                changes.append(
                    {
                        "field": f"post_pivot_formula_metrics.{field_name}",
                        "from": operand,
                        "to": repaired_operand,
                        "reason": "resolved_post_pivot_operand",
                    }
                )
            formula[field_name] = repaired_operand
        if not valid:
            changes.append(
                {
                    "field": "post_pivot_formula_metrics",
                    "from": formula,
                    "to": None,
                    "reason": "dropped_unresolved_post_pivot_formula_operand",
                }
            )
            continue
        repaired_post_pivot_formula_metrics.append(formula)
        alias = str(formula.get("as_name") or "").strip()
        if alias:
            post_pivot_columns.append(alias)
    data["post_pivot_formula_metrics"] = repaired_post_pivot_formula_metrics

    repaired_post_pivot_having = []
    for flt in data.get("post_pivot_having") or []:
        resolved = best_column(str(flt.get("col") or ""), post_pivot_columns, base_profiles)
        if resolved:
            repaired_post_pivot_having.append({**flt, "col": resolved})
            if resolved != flt.get("col"):
                changes.append(
                    {
                        "field": "post_pivot_having.col",
                        "from": flt.get("col"),
                        "to": resolved,
                        "reason": "resolved_post_pivot_having_column",
                    }
                )
        else:
            changes.append({"field": "post_pivot_having", "from": flt, "to": None, "reason": "dropped_unresolved_post_pivot_having"})
    data["post_pivot_having"] = repaired_post_pivot_having

    order_scope = post_pivot_columns if data.get("pivot") else pre_pivot_output_columns
    order_by = data.get("order_by")
    if order_by:
        resolved = best_column(str(order_by.get("col") or ""), order_scope, base_profiles)
        if resolved:
            data["order_by"] = {**order_by, "col": resolved}
            if resolved != order_by.get("col"):
                changes.append({"field": "order_by.col", "from": order_by.get("col"), "to": resolved, "reason": "resolved_order_by_column"})
        else:
            changes.append({"field": "order_by", "from": order_by, "to": None, "reason": "dropped_unresolved_order_by"})
            data["order_by"] = None

    if data.get("return_rows"):
        if data.get("groupby"):
            changes.append({"field": "groupby", "from": data["groupby"], "to": [], "reason": "detail_mode_no_groupby"})
            data["groupby"] = []
        if data.get("metrics"):
            changes.append({"field": "metrics", "from": data["metrics"], "to": [], "reason": "detail_mode_no_metrics"})
            data["metrics"] = []
    elif not data.get("metrics"):
        default_metric = Metric(agg="count_rows", col=None, as_name="count").model_dump()
        changes.append({"field": "metrics", "from": [], "to": [default_metric], "reason": "default_metric"})
        data["metrics"] = [default_metric]

    return TransformPlan.model_validate(data), {"changes": changes}
