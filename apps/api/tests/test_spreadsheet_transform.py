from __future__ import annotations

import pandas as pd

from app.services.spreadsheet.core.schema import Filter, FormulaMetric, Metric, PivotSpec, SelectionPlan, Sort, TransformPlan
from app.services.spreadsheet.execution.exact_executor import execute_exact_plan
from app.services.spreadsheet.execution.exact_support import exact_execution_support
from app.services.spreadsheet.execution.executor import apply_transform
from app.services.spreadsheet.pipeline.column_profile import attach_column_profiles
from app.services.spreadsheet.quality.repair import repair_transform_plan
from app.services.spreadsheet.quality.validator import validate_transform_plan


def _advanced_df() -> pd.DataFrame:
    return attach_column_profiles(
        pd.DataFrame(
            {
                "Region": ["East", "East", "East", "West", "West", "West"],
                "Month": ["2025-01", "2025-01", "2025-02", "2025-01", "2025-02", "2025-02"],
                "Revenue": [60, 40, 120, 80, 90, 70],
                "Cost": [30, 30, 70, 40, 50, 50],
            }
        )
    )


def _advanced_transform_plan() -> TransformPlan:
    return TransformPlan(
        groupby=["Region", "Month"],
        metrics=[
            Metric(agg="sum", col="Revenue", as_name="revenue"),
            Metric(agg="sum", col="Cost", as_name="cost"),
        ],
        formula_metrics=[FormulaMetric(as_name="profit", op="sub", left="revenue", right="cost")],
        having=[Filter(col="profit", op=">", value=30)],
        pivot=PivotSpec(index=["Region"], columns="Month", values="profit"),
        post_pivot_formula_metrics=[FormulaMetric(as_name="delta", op="sub", left="2025-02", right="2025-01")],
        post_pivot_having=[Filter(col="delta", op=">", value=10)],
        order_by=Sort(col="delta", dir="desc"),
    )


def test_apply_transform_supports_pivot_formula_and_post_pivot_workflow() -> None:
    result_df, meta = apply_transform(_advanced_df(), _advanced_transform_plan())

    assert result_df.columns.tolist() == ["Region", "2025-01", "2025-02", "delta"]
    assert result_df.to_dict(orient="records") == [
        {"Region": "West", "2025-01": 40.0, "2025-02": 60.0, "delta": 20.0}
    ]
    assert "formula_metrics" in meta
    assert "pivot" in meta
    assert "post_pivot_formula_metrics" in meta
    assert "post_pivot_having" in meta


def test_validate_transform_plan_rejects_post_pivot_without_pivot() -> None:
    issues = validate_transform_plan(
        _advanced_df(),
        TransformPlan(
            metrics=[Metric(agg="sum", col="Revenue", as_name="revenue")],
            post_pivot_formula_metrics=[FormulaMetric(as_name="delta", op="sub", left="A", right="B")],
        ),
        question="Compare months",
        mode="text",
    )

    issue_kinds = {issue["kind"] for issue in issues}
    assert "post_pivot_requires_pivot" in issue_kinds


def test_repair_transform_plan_repairs_pivot_and_post_pivot_references() -> None:
    repaired, meta = repair_transform_plan(
        _advanced_df(),
        TransformPlan(
            groupby=["Regoin", "Monthh"],
            metrics=[
                Metric(agg="sum", col="Revenuee", as_name="revenue"),
                Metric(agg="sum", col="Costs", as_name="cost"),
            ],
            formula_metrics=[FormulaMetric(as_name="profit", op="sub", left="revenue", right="cost")],
            pivot=PivotSpec(index=["Regoin"], columns="Monthh", values="Profitt"),
            post_pivot_formula_metrics=[FormulaMetric(as_name="delta", op="sub", left="2025-02", right="2025-01")],
            order_by=Sort(col="Deltaa", dir="desc"),
        ),
        question="Compare month deltas by region",
        mode="text",
    )

    assert repaired.groupby == ["Region", "Month"]
    assert repaired.metrics[0].col == "Revenue"
    assert repaired.metrics[1].col == "Cost"
    assert repaired.formula_metrics[0].left == "revenue"
    assert repaired.pivot is not None
    assert repaired.pivot.index == ["Region"]
    assert repaired.pivot.columns == "Month"
    assert repaired.pivot.values == "profit"
    assert repaired.order_by is not None
    assert repaired.order_by.col == "delta"
    assert meta["changes"]


def test_exact_execution_supports_advanced_transform_plan() -> None:
    support = exact_execution_support(
        selection_plan=SelectionPlan(),
        transform_plan=_advanced_transform_plan(),
    )

    assert support["eligible"] is True

    result_df, meta = execute_exact_plan(
        _advanced_df(),
        selection_plan=SelectionPlan(),
        transform_plan=_advanced_transform_plan(),
    )

    assert result_df.to_dict(orient="records") == [
        {"Region": "West", "2025-01": 40.0, "2025-02": 60.0, "delta": 20.0}
    ]
    assert meta["eligible"] is True
