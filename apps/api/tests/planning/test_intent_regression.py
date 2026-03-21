from __future__ import annotations

import pytest

from app.services.spreadsheet.planning.intent_regression import (
    DEFAULT_INTENT_CASES_PATH,
    IntentRegressionCase,
    build_intent_regression_failure_snapshot,
    evaluate_intent_regression_case,
    load_intent_regression_cases,
)


def _load_cases() -> list[IntentRegressionCase]:
    return load_intent_regression_cases(DEFAULT_INTENT_CASES_PATH)


def test_intent_regression_fixture_covers_required_categories() -> None:
    cases = _load_cases()
    categories = {case.category for case in cases}
    assert {"compare", "filter", "time_agg", "topn", "detail_summary"} <= categories


@pytest.mark.parametrize("case", _load_cases(), ids=lambda case: case.id)
def test_intent_regression_cases(case: IntentRegressionCase) -> None:
    result = evaluate_intent_regression_case(case)
    assert result.passed, (
        f"{case.id} failed: {result.failures}; "
        f"actual_intent={result.draft.intent}; "
        f"actual_mode={result.draft.mode}; "
        f"planner_meta={result.draft.planner_meta}"
    )


def test_intent_regression_failure_snapshot_contains_expected_context() -> None:
    case = IntentRegressionCase(
        id="snapshot_failure_case",
        category="topn",
        dataset="billing",
        chat_text="Show the top 3 services by amount.",
        requested_mode="auto",
        expected_intent="share",
    )
    result = evaluate_intent_regression_case(case)
    assert not result.passed

    snapshot = build_intent_regression_failure_snapshot([result])
    assert len(snapshot) == 1
    item = snapshot[0]
    assert item["id"] == case.id
    assert item["expected"]["intent"] == "share"
    assert item["actual"]["intent"] == "ranking"
    assert isinstance(item["actual"]["planner_meta"], dict)
    assert isinstance(item["actual"]["selection_plan"], dict)
    assert isinstance(item["actual"]["transform_plan"], dict)
    assert item["failures"]
