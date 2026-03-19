from __future__ import annotations

from typing import Any

from ..core.schema import Metric, TransformPlan
from ..execution.executor import apply_transform
from ..pipeline.column_profile import get_column_profiles
from .validator_common import (
    derived_semantic_type,
    has_error,
    is_numeric_literal,
    issue,
    plain_row_count_intent,
    register_output_alias,
    resolve_ref_name,
    semantic_type,
    try_validate_transform_runtime,
    validate_operand,
    validate_ref_with_scope,
    virtual_profiles,
)


def infer_pivot_output_scope(df: Any, plan: TransformPlan, max_rows: int = 500) -> dict[str, Any]:
    if not plan.pivot:
        return {"inferred": False, "pivot_index_columns": [], "generated_columns": [], "final_columns": []}
    try:
        preview_df = df.head(max_rows) if hasattr(df, "head") else df
        partial_plan = TransformPlan.model_validate(
            {
                **plan.model_dump(),
                "post_pivot_formula_metrics": [],
                "post_pivot_having": [],
                "order_by": None,
                "top_k": None,
            }
        )
        pivot_df, pivot_meta = apply_transform(preview_df, partial_plan)
        final_columns = [str(column) for column in getattr(pivot_df, "columns", [])]
        pivot_meta_info = pivot_meta.get("pivot") or {}
        pivot_index_columns = [str(column) for column in (pivot_meta_info.get("index") or [])]
        generated_columns = [column for column in final_columns if column not in pivot_index_columns]
        return {
            "inferred": True,
            "pivot_index_columns": pivot_index_columns,
            "generated_columns": generated_columns,
            "final_columns": final_columns,
        }
    except Exception as exc:
        return {
            "inferred": False,
            "pivot_index_columns": [],
            "generated_columns": [],
            "final_columns": [],
            "error": str(exc),
        }


def validate_transform_plan(df: Any, plan: TransformPlan, *, question: str, mode: str) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []

    if mode == "text" and plain_row_count_intent(question):
        metrics = list(plan.metrics or [])
        is_plain_count_rows = (
            len(metrics) == 1
            and str(metrics[0].agg or "") == "count_rows"
            and not metrics[0].col
            and not (plan.groupby or [])
            and not plan.return_rows
            and not (plan.formula_metrics or [])
            and not (plan.having or [])
            and plan.pivot is None
            and not (plan.post_pivot_formula_metrics or [])
            and not (plan.post_pivot_having or [])
            and plan.order_by is None
            and plan.top_k is None
        )
        if not is_plain_count_rows:
            issues.append(
                issue(
                    "plain_row_count_transform",
                    "Plain row-count questions should use a single count_rows metric without groupby, return_rows, pivot, order_by, or top_k.",
                    field="transform",
                )
            )

    base_columns = [str(column) for column in getattr(df, "columns", [])]
    base_profiles = get_column_profiles(df)
    available_input_columns = list(base_columns)
    input_semantic_types = {name: str((base_profiles.get(name) or {}).get("semantic_type") or "unknown") for name in base_columns}

    for index, item in enumerate(plan.derived_columns or []):
        field = f"derived_columns[{index}]"
        current_profiles = virtual_profiles(available_input_columns, base_profiles, input_semantic_types)
        alias = str(item.as_name or "").strip()

        if not alias:
            issues.append(issue("missing_alias", "Missing output alias.", field=f"{field}.as_name"))
        elif alias in available_input_columns:
            issues.append(issue("duplicate_alias", f"Duplicate output alias: {alias}", field=f"{field}.as_name", requested=alias))
            alias = ""

        if item.kind == "date_bucket":
            if not item.source_col:
                issues.append(issue("missing_source_col", "date_bucket requires source_col.", field=f"{field}.source_col"))
            else:
                ref_issues = validate_ref_with_scope(
                    item.source_col,
                    field=f"{field}.source_col",
                    columns=available_input_columns,
                    profiles=current_profiles,
                )
                issues.extend(ref_issues)
                if not has_error(ref_issues):
                    resolved = resolve_ref_name(item.source_col, columns=available_input_columns, profiles=current_profiles)
                    source_semantic_type = semantic_type(resolved, semantic_types=input_semantic_types, base_profiles=base_profiles)
                    if source_semantic_type not in {"date", "unknown"}:
                        issues.append(
                            issue(
                                "non_date_bucket_source",
                                f"date_bucket is usually expected to use a date column, got semantic type '{source_semantic_type}'.",
                                severity="warn",
                                field=f"{field}.source_col",
                                requested=item.source_col,
                                resolved=resolved,
                            )
                        )
            if not item.grain:
                issues.append(issue("missing_grain", "date_bucket requires grain.", field=f"{field}.grain"))
        elif item.kind == "arithmetic":
            issues.extend(validate_operand(item.left, field=f"{field}.left", columns=available_input_columns, profiles=current_profiles))
            issues.extend(validate_operand(item.right, field=f"{field}.right", columns=available_input_columns, profiles=current_profiles))
            if not item.op:
                issues.append(issue("missing_op", "arithmetic requires op.", field=f"{field}.op"))
        else:
            issues.append(issue("unsupported_derived_kind", f"Unsupported derived column kind: {item.kind}", field=f"{field}.kind"))

        if alias:
            available_input_columns.append(alias)
            input_semantic_types[alias] = derived_semantic_type(item.kind, item.grain)

    input_profiles = virtual_profiles(available_input_columns, base_profiles, input_semantic_types)

    detail_mode = bool(plan.return_rows)
    if mode == "chart" and detail_mode:
        issues.append(issue("chart_return_rows", "Chart mode does not support return_rows=true.", field="return_rows"))
    if detail_mode and plan.groupby:
        issues.append(issue("detail_mode_groupby", "return_rows=true cannot be combined with groupby.", field="groupby"))
    if detail_mode and plan.metrics:
        issues.append(issue("detail_mode_metrics", "return_rows=true cannot be combined with metrics.", field="metrics"))

    output_columns: list[str] = []
    output_semantic_types: dict[str, str] = {}
    metrics: list[Metric] = []

    if detail_mode:
        output_columns = list(available_input_columns)
        output_semantic_types = dict(input_semantic_types)
    else:
        for index, column in enumerate(plan.groupby or []):
            ref_issues = validate_ref_with_scope(column, field=f"groupby[{index}]", columns=available_input_columns, profiles=input_profiles)
            issues.extend(ref_issues)
            if not has_error(ref_issues):
                resolved = resolve_ref_name(column, columns=available_input_columns, profiles=input_profiles)
                if resolved:
                    if resolved in output_columns:
                        issues.append(
                            issue(
                                "duplicate_groupby",
                                f"Duplicate groupby column: {column} -> {resolved}",
                                severity="warn",
                                field=f"groupby[{index}]",
                                requested=column,
                                resolved=resolved,
                            )
                        )
                    else:
                        output_columns.append(resolved)
                        output_semantic_types[resolved] = semantic_type(
                            resolved,
                            semantic_types=input_semantic_types,
                            base_profiles=base_profiles,
                        )

        metrics = plan.metrics or [Metric(agg="count_rows", col=None, as_name="count")]
        for index, metric in enumerate(metrics):
            field = f"metrics[{index}]"
            if metric.col:
                ref_issues = validate_ref_with_scope(
                    metric.col,
                    field=f"{field}.col",
                    columns=available_input_columns,
                    profiles=input_profiles,
                )
                issues.extend(ref_issues)
                if not has_error(ref_issues):
                    resolved = resolve_ref_name(metric.col, columns=available_input_columns, profiles=input_profiles)
                    metric_semantic_type = semantic_type(resolved, semantic_types=input_semantic_types, base_profiles=base_profiles)
                    if metric.agg in {"sum", "avg"} and metric_semantic_type not in {"numeric", "unknown"}:
                        issues.append(
                            issue(
                                "non_numeric_metric",
                                f"{metric.agg} is usually expected to use a numeric column, got semantic type '{metric_semantic_type}'.",
                                field=f"{field}.col",
                                requested=metric.col,
                                resolved=resolved,
                            )
                        )
            elif metric.agg not in {"count_rows"}:
                issues.append(issue("missing_metric_col", f"{metric.agg} requires col.", field=f"{field}.col"))

            issues.extend(
                register_output_alias(
                    str(metric.as_name or "").strip(),
                    field=f"{field}.as_name",
                    columns=output_columns,
                    semantic_types=output_semantic_types,
                    semantic_type_name="numeric",
                )
            )

    post_profiles = virtual_profiles(output_columns, base_profiles, output_semantic_types)

    for index, formula in enumerate(plan.formula_metrics or []):
        field = f"formula_metrics[{index}]"
        issues.extend(validate_operand(formula.left, field=f"{field}.left", columns=output_columns, profiles=post_profiles))
        issues.extend(validate_operand(formula.right, field=f"{field}.right", columns=output_columns, profiles=post_profiles))
        if not formula.op:
            issues.append(issue("missing_op", "formula_metric requires op.", field=f"{field}.op"))
        issues.extend(
            register_output_alias(
                str(formula.as_name or "").strip(),
                field=f"{field}.as_name",
                columns=output_columns,
                semantic_types=output_semantic_types,
                semantic_type_name="numeric",
            )
        )
        post_profiles = virtual_profiles(output_columns, base_profiles, output_semantic_types)

    for index, flt in enumerate(plan.having or []):
        ref_issues = validate_ref_with_scope(flt.col, field=f"having[{index}].col", columns=output_columns, profiles=post_profiles)
        issues.extend(ref_issues)
        if not has_error(ref_issues) and flt.op in {">", ">=", "<", "<="}:
            resolved = resolve_ref_name(flt.col, columns=output_columns, profiles=post_profiles)
            having_semantic_type = semantic_type(resolved, semantic_types=output_semantic_types, base_profiles=base_profiles)
            if having_semantic_type not in {"numeric", "date", "unknown"}:
                issues.append(
                    issue(
                        "non_numeric_having",
                        f"{flt.op} filters are usually expected to use numeric or date-like columns, got semantic type '{having_semantic_type}'.",
                        severity="warn",
                        field=f"having[{index}].col",
                        requested=flt.col,
                        resolved=resolved,
                    )
                )

    pivot_index_columns: list[str] = []
    if plan.pivot:
        for index, column in enumerate(plan.pivot.index or []):
            ref_issues = validate_ref_with_scope(column, field=f"pivot.index[{index}]", columns=output_columns, profiles=post_profiles)
            issues.extend(ref_issues)
            if not has_error(ref_issues):
                resolved = resolve_ref_name(column, columns=output_columns, profiles=post_profiles)
                if resolved and resolved not in pivot_index_columns:
                    pivot_index_columns.append(resolved)

        pivot_col_issues = validate_ref_with_scope(plan.pivot.columns, field="pivot.columns", columns=output_columns, profiles=post_profiles)
        issues.extend(pivot_col_issues)

        value_col_issues = validate_ref_with_scope(plan.pivot.values, field="pivot.values", columns=output_columns, profiles=post_profiles)
        issues.extend(value_col_issues)
        if not has_error(value_col_issues):
            resolved_value = resolve_ref_name(plan.pivot.values, columns=output_columns, profiles=post_profiles)
            value_semantic_type = semantic_type(resolved_value, semantic_types=output_semantic_types, base_profiles=base_profiles)
            if value_semantic_type not in {"numeric", "unknown"}:
                issues.append(
                    issue(
                        "pivot_non_numeric_value",
                        f"pivot.values is usually expected to be numeric, got semantic type '{value_semantic_type}'.",
                        severity="warn",
                        field="pivot.values",
                        requested=plan.pivot.values,
                        resolved=resolved_value,
                    )
                )

    if plan.post_pivot_formula_metrics and not plan.pivot:
        issues.append(issue("post_pivot_requires_pivot", "post_pivot_formula_metrics requires pivot.", field="post_pivot_formula_metrics"))
    if plan.post_pivot_having and not plan.pivot:
        issues.append(issue("post_pivot_requires_pivot", "post_pivot_having requires pivot.", field="post_pivot_having"))

    pivot_scope = infer_pivot_output_scope(df, plan) if plan.pivot else {
        "inferred": False,
        "pivot_index_columns": [],
        "generated_columns": [],
        "final_columns": [],
    }

    after_pivot_columns = list(output_columns)
    after_pivot_semantic_types = dict(output_semantic_types)

    if plan.pivot and pivot_scope.get("inferred"):
        after_pivot_columns = [str(column) for column in (pivot_scope.get("final_columns") or [])]
        generated = {str(column) for column in (pivot_scope.get("generated_columns") or [])}
        after_pivot_semantic_types = {
            column: (
                "numeric"
                if column in generated
                else semantic_type(column, semantic_types=output_semantic_types, base_profiles=base_profiles)
            )
            for column in after_pivot_columns
        }
    elif plan.pivot:
        after_pivot_columns = list(pivot_index_columns)
        after_pivot_semantic_types = {
            column: semantic_type(column, semantic_types=output_semantic_types, base_profiles=base_profiles)
            for column in after_pivot_columns
        }

    after_pivot_profiles = virtual_profiles(after_pivot_columns, base_profiles, after_pivot_semantic_types)

    if plan.pivot:
        for index, formula in enumerate(plan.post_pivot_formula_metrics or []):
            field = f"post_pivot_formula_metrics[{index}]"
            for operand_name, operand in (("left", formula.left), ("right", formula.right)):
                if is_numeric_literal(operand):
                    continue
                operand_text = str(operand or "").strip()
                if pivot_scope.get("inferred"):
                    issues.extend(
                        validate_ref_with_scope(
                            operand_text,
                            field=f"{field}.{operand_name}",
                            columns=after_pivot_columns,
                            profiles=after_pivot_profiles,
                        )
                    )
                elif operand_text not in after_pivot_columns:
                    issues.append(
                        issue(
                            "pivot_generated_column_unverified",
                            "Pivot-generated columns could not be statically verified; runtime validation will decide.",
                            severity="warn",
                            field=f"{field}.{operand_name}",
                            requested=operand_text,
                        )
                    )
            if not formula.op:
                issues.append(issue("missing_op", "post_pivot_formula_metric requires op.", field=f"{field}.op"))
            issues.extend(
                register_output_alias(
                    str(formula.as_name or "").strip(),
                    field=f"{field}.as_name",
                    columns=after_pivot_columns,
                    semantic_types=after_pivot_semantic_types,
                    semantic_type_name="numeric",
                )
            )
            after_pivot_profiles = virtual_profiles(after_pivot_columns, base_profiles, after_pivot_semantic_types)

        for index, flt in enumerate(plan.post_pivot_having or []):
            operand_text = str(flt.col or "").strip()
            if pivot_scope.get("inferred"):
                ref_issues = validate_ref_with_scope(
                    operand_text,
                    field=f"post_pivot_having[{index}].col",
                    columns=after_pivot_columns,
                    profiles=after_pivot_profiles,
                )
                issues.extend(ref_issues)
                if not has_error(ref_issues) and flt.op in {">", ">=", "<", "<="}:
                    resolved = resolve_ref_name(operand_text, columns=after_pivot_columns, profiles=after_pivot_profiles)
                    post_having_semantic_type = semantic_type(resolved, semantic_types=after_pivot_semantic_types, base_profiles=base_profiles)
                    if post_having_semantic_type not in {"numeric", "date", "unknown"}:
                        issues.append(
                            issue(
                                "non_numeric_having",
                                f"{flt.op} filters are usually expected to use numeric or date-like columns, got semantic type '{post_having_semantic_type}'.",
                                severity="warn",
                                field=f"post_pivot_having[{index}].col",
                                requested=flt.col,
                                resolved=resolved,
                            )
                        )
            elif operand_text not in after_pivot_columns:
                issues.append(
                    issue(
                        "pivot_generated_column_unverified",
                        "Pivot-generated columns could not be statically verified; runtime validation will decide.",
                        severity="warn",
                        field=f"post_pivot_having[{index}].col",
                        requested=flt.col,
                    )
                )

    final_columns = after_pivot_columns if plan.pivot else output_columns
    final_semantic_types = after_pivot_semantic_types if plan.pivot else output_semantic_types
    final_profiles = virtual_profiles(final_columns, base_profiles, final_semantic_types)

    if plan.order_by:
        if plan.pivot and not final_columns:
            issues.append(
                issue(
                    "pivot_order_by_unverified",
                    "Pivot output columns could not be statically verified for order_by; runtime validation will decide.",
                    severity="warn",
                    field="order_by.col",
                    requested=plan.order_by.col,
                )
            )
        else:
            order_issues = validate_ref_with_scope(plan.order_by.col, field="order_by.col", columns=final_columns, profiles=final_profiles)
            if plan.pivot and has_error(order_issues):
                issues.append(
                    issue(
                        "pivot_order_by_unverified",
                        "Pivot output columns could not be statically verified for order_by; runtime validation will decide.",
                        severity="warn",
                        field="order_by.col",
                        requested=plan.order_by.col,
                    )
                )
            else:
                issues.extend(order_issues)

    if plan.top_k is not None and int(plan.top_k) <= 0:
        issues.append(issue("invalid_top_k", "top_k must be greater than 0.", field="top_k"))

    if not has_error(issues):
        issues.extend(try_validate_transform_runtime(df, plan))
    return issues
