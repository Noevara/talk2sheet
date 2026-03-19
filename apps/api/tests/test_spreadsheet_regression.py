from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from app.services.spreadsheet.analysis import analyze
from app.services.spreadsheet.pipeline.column_profile import attach_column_profiles


def _sample_df() -> pd.DataFrame:
    return attach_column_profiles(
        pd.DataFrame(
            {
                "Date": ["2025-01-01", "2025-01-10", "2025-02-03", "2025-02-15"],
                "Category": ["A", "B", "A", "C"],
                "Amount": [100, 50, 80, 20],
            }
        )
    )


def _billing_df() -> pd.DataFrame:
    return attach_column_profiles(
        pd.DataFrame(
            {
                "Date": ["2025-01-03", "2025-01-04", "2025-01-06", "2025-01-11", "2025-02-03"],
                "Service Name": ["Compute", "Storage", "Compute", "AI", "Compute"],
                "Billing Item Name": ["Instance", "Disk", "Instance", "Token", "Bandwidth"],
                "Region": ["cn-sh", "cn-bj", "cn-sh", "us-west", "cn-bj"],
                "Transaction ID": ["T-101", "T-102", "T-103", "T-104", "T-105"],
                "Amount": [120, 80, 60, 200, 40],
            }
        )
    )


def _load_cases(name: str) -> list[dict[str, object]]:
    fixture_path = Path(__file__).with_name("fixtures") / name
    return json.loads(fixture_path.read_text(encoding="utf-8"))


@pytest.mark.parametrize("case", _load_cases("analysis_cases.json"), ids=lambda case: str(case["id"]))
def test_analysis_regression_cases(case: dict[str, object]) -> None:
    result = analyze(
        _sample_df(),
        chat_text=str(case["chat_text"]),
        requested_mode=str(case["requested_mode"]),
        locale=str(case["locale"]),
        rows_loaded=4,
    )

    assert result.pipeline["planner"]["intent"] == case["expected_intent"]

    expected_answer_contains = case.get("expected_answer_contains")
    if expected_answer_contains is not None:
        assert str(expected_answer_contains) in result.answer

    expected_preview_rows = case.get("expected_preview_rows")
    if expected_preview_rows is not None:
        assert len(result.pipeline.get("preview_rows") or []) == int(expected_preview_rows)

    expected_chart_type = case.get("expected_chart_type")
    if expected_chart_type is not None:
        assert result.chart_spec is not None
        assert result.chart_spec["type"] == expected_chart_type

    expected_chart_rows = case.get("expected_chart_rows")
    if expected_chart_rows is not None:
        assert len(result.chart_data or []) == int(expected_chart_rows)


@pytest.mark.parametrize("case", _load_cases("analysis_parity_cases.json"), ids=lambda case: str(case["id"]))
def test_analysis_parity_regression_cases(case: dict[str, object]) -> None:
    result = analyze(
        _billing_df(),
        chat_text=str(case["chat_text"]),
        requested_mode=str(case["requested_mode"]),
        locale=str(case["locale"]),
        rows_loaded=5,
    )

    assert result.pipeline["planner"]["intent"] == case["expected_intent"]

    expected_mode = case.get("expected_mode")
    if expected_mode is not None:
        assert result.mode == expected_mode

    expected_result_columns = case.get("expected_result_columns")
    if expected_result_columns is not None:
        assert result.pipeline["result_columns"] == expected_result_columns

    expected_answer_contains = case.get("expected_answer_contains")
    if expected_answer_contains is not None:
        assert str(expected_answer_contains) in result.answer

    expected_chart_type = case.get("expected_chart_type")
    if expected_chart_type is not None:
        assert result.chart_spec is not None
        assert result.chart_spec["type"] == expected_chart_type
