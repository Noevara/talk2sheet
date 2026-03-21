from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any

import pandas as pd

from ..pipeline.column_profile import attach_column_profiles
from .planner import HeuristicPlanner
from .planner_types import PlanDraft


ROOT = Path(__file__).resolve().parents[6]
DEFAULT_INTENT_CASES_PATH = ROOT / "apps" / "api" / "tests" / "fixtures" / "intent_regression_cases.v0.3.0.json"


@dataclass(frozen=True)
class IntentRegressionCase:
    id: str
    category: str
    scenario: str
    dataset: str
    chat_text: str
    requested_mode: str
    expected_intent: str
    expected_mode: str | None = None
    expected_compare_basis: str | None = None
    expected_comparison_type: str | None = None
    expected_bucket_grain: str | None = None
    expected_top_k: int | None = None
    expected_requested_period: str | None = None
    expected_recent_period_count: int | None = None
    expected_value_filters: list[dict[str, str]] | None = None
    expected_followup_reused_previous_plan: bool | None = None
    expected_reuse_strategy: str | None = None
    followup_context: dict[str, Any] | None = None


@dataclass(frozen=True)
class IntentRegressionResult:
    case: IntentRegressionCase
    draft: PlanDraft
    failures: list[str]

    @property
    def passed(self) -> bool:
        return not self.failures


def load_intent_regression_cases(path: Path) -> list[IntentRegressionCase]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError(f"Intent regression fixture must be a list: {path}")
    cases: list[IntentRegressionCase] = []
    for item in payload:
        if not isinstance(item, dict):
            raise ValueError(f"Intent regression case must be an object: {item!r}")
        cases.append(
            IntentRegressionCase(
                id=str(item.get("id") or "").strip(),
                category=str(item.get("category") or "").strip(),
                scenario=str(item.get("scenario") or "").strip() or "single_sheet",
                dataset=str(item.get("dataset") or "").strip(),
                chat_text=str(item.get("chat_text") or "").strip(),
                requested_mode=str(item.get("requested_mode") or "auto").strip() or "auto",
                expected_intent=str(item.get("expected_intent") or "").strip(),
                expected_mode=_optional_string(item.get("expected_mode")),
                expected_compare_basis=_optional_string(item.get("expected_compare_basis")),
                expected_comparison_type=_optional_string(item.get("expected_comparison_type")),
                expected_bucket_grain=_optional_string(item.get("expected_bucket_grain")),
                expected_top_k=_optional_int(item.get("expected_top_k")),
                expected_requested_period=_optional_string(item.get("expected_requested_period")),
                expected_recent_period_count=_optional_int(item.get("expected_recent_period_count")),
                expected_value_filters=_optional_filter_list(item.get("expected_value_filters")),
                expected_followup_reused_previous_plan=_optional_bool(item.get("expected_followup_reused_previous_plan")),
                expected_reuse_strategy=_optional_string(item.get("expected_reuse_strategy")),
                followup_context=_optional_context(item.get("followup_context")),
            )
        )
    return cases


def evaluate_intent_regression_case(
    case: IntentRegressionCase,
    *,
    planner: HeuristicPlanner | None = None,
) -> IntentRegressionResult:
    resolved_planner = planner or HeuristicPlanner()
    draft = resolved_planner.plan(
        _build_dataset(case.dataset),
        chat_text=case.chat_text,
        requested_mode=case.requested_mode,
        followup_context=case.followup_context,
    )
    failures: list[str] = []

    if draft.intent != case.expected_intent:
        failures.append(f"intent expected={case.expected_intent} actual={draft.intent}")

    if case.expected_mode and draft.mode != case.expected_mode:
        failures.append(f"mode expected={case.expected_mode} actual={draft.mode}")

    if case.expected_compare_basis:
        actual = str(draft.planner_meta.get("compare_basis") or "")
        if actual != case.expected_compare_basis:
            failures.append(f"compare_basis expected={case.expected_compare_basis} actual={actual}")

    if case.expected_comparison_type:
        actual = str(draft.planner_meta.get("comparison_type") or "")
        if actual != case.expected_comparison_type:
            failures.append(f"comparison_type expected={case.expected_comparison_type} actual={actual}")

    if case.expected_bucket_grain:
        actual = str(draft.planner_meta.get("bucket_grain") or draft.planner_meta.get("compare_grain") or "")
        if actual != case.expected_bucket_grain:
            failures.append(f"bucket_grain expected={case.expected_bucket_grain} actual={actual}")

    if case.expected_top_k is not None:
        actual_top_k = draft.transform_plan.top_k
        if actual_top_k is None:
            raw_top_k = draft.planner_meta.get("top_k")
            actual_top_k = int(raw_top_k) if isinstance(raw_top_k, int) else None
        if actual_top_k != case.expected_top_k:
            failures.append(f"top_k expected={case.expected_top_k} actual={actual_top_k}")

    if case.expected_requested_period:
        actual = str(draft.planner_meta.get("requested_period") or "")
        if actual != case.expected_requested_period:
            failures.append(f"requested_period expected={case.expected_requested_period} actual={actual}")

    if case.expected_recent_period_count is not None:
        actual_count = draft.planner_meta.get("requested_recent_period_count")
        actual = int(actual_count) if isinstance(actual_count, int) else None
        if actual != case.expected_recent_period_count:
            failures.append(f"recent_period_count expected={case.expected_recent_period_count} actual={actual}")

    if case.expected_value_filters:
        actual_filters = _normalize_value_filters(draft.planner_meta.get("value_filters"))
        for expected_filter in case.expected_value_filters:
            if expected_filter not in actual_filters:
                failures.append(
                    f"value_filter missing={expected_filter} actual={actual_filters}"
                )
    if case.expected_followup_reused_previous_plan is not None:
        actual_reused = bool(draft.planner_meta.get("followup_reused_previous_plan"))
        if actual_reused != bool(case.expected_followup_reused_previous_plan):
            failures.append(
                f"followup_reused_previous_plan expected={bool(case.expected_followup_reused_previous_plan)} actual={actual_reused}"
            )
    if case.expected_reuse_strategy:
        actual_strategy = str(draft.planner_meta.get("reuse_strategy") or "")
        if actual_strategy != case.expected_reuse_strategy:
            failures.append(
                f"reuse_strategy expected={case.expected_reuse_strategy} actual={actual_strategy}"
            )

    return IntentRegressionResult(case=case, draft=draft, failures=failures)


def evaluate_intent_regression_cases(
    cases: list[IntentRegressionCase],
    *,
    planner: HeuristicPlanner | None = None,
) -> list[IntentRegressionResult]:
    resolved_planner = planner or HeuristicPlanner()
    return [evaluate_intent_regression_case(case, planner=resolved_planner) for case in cases]


def summarize_intent_regression_results(results: list[IntentRegressionResult]) -> dict[str, Any]:
    total = len(results)
    passed = sum(1 for item in results if item.passed)
    failed = total - passed
    categories: dict[str, dict[str, int]] = {}
    scenarios: dict[str, dict[str, int]] = {}
    for result in results:
        key = result.case.category or "uncategorized"
        bucket = categories.setdefault(key, {"total": 0, "passed": 0})
        bucket["total"] += 1
        if result.passed:
            bucket["passed"] += 1
        scenario_key = result.case.scenario or "unspecified"
        scenario_bucket = scenarios.setdefault(scenario_key, {"total": 0, "passed": 0})
        scenario_bucket["total"] += 1
        if result.passed:
            scenario_bucket["passed"] += 1
    for bucket in categories.values():
        bucket["failed"] = bucket["total"] - bucket["passed"]
    for bucket in scenarios.values():
        bucket["failed"] = bucket["total"] - bucket["passed"]
    return {
        "total": total,
        "passed": passed,
        "failed": failed,
        "pass_rate": (passed / total) if total else 0.0,
        "categories": categories,
        "scenarios": scenarios,
    }


def build_intent_regression_failure_snapshot(results: list[IntentRegressionResult]) -> list[dict[str, Any]]:
    snapshot: list[dict[str, Any]] = []
    for result in results:
        if result.passed:
            continue
        snapshot.append(
            {
                "id": result.case.id,
                "category": result.case.category,
                "scenario": result.case.scenario,
                "dataset": result.case.dataset,
                "chat_text": result.case.chat_text,
                "requested_mode": result.case.requested_mode,
                "followup_context": result.case.followup_context or {},
                "expected": {
                    "intent": result.case.expected_intent,
                    "mode": result.case.expected_mode,
                    "compare_basis": result.case.expected_compare_basis,
                    "comparison_type": result.case.expected_comparison_type,
                    "bucket_grain": result.case.expected_bucket_grain,
                    "top_k": result.case.expected_top_k,
                    "requested_period": result.case.expected_requested_period,
                    "recent_period_count": result.case.expected_recent_period_count,
                    "value_filters": result.case.expected_value_filters,
                    "followup_reused_previous_plan": result.case.expected_followup_reused_previous_plan,
                    "reuse_strategy": result.case.expected_reuse_strategy,
                },
                "actual": {
                    "intent": result.draft.intent,
                    "mode": result.draft.mode,
                    "planner_meta": result.draft.planner_meta,
                    "selection_plan": result.draft.selection_plan.model_dump(),
                    "transform_plan": result.draft.transform_plan.model_dump(),
                    "chart_spec": result.draft.chart_spec.model_dump() if result.draft.chart_spec is not None else None,
                },
                "failures": result.failures,
            }
        )
    return snapshot


def _build_dataset(name: str):
    dataset = str(name or "").strip().lower()
    if dataset == "sample":
        return _sample_df()
    if dataset == "billing":
        return _billing_df()
    if dataset == "monthly_compare":
        return _monthly_compare_df()
    raise ValueError(f"Unknown intent regression dataset: {name}")


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


def _monthly_compare_df() -> pd.DataFrame:
    return attach_column_profiles(
        pd.DataFrame(
            {
                "Date": ["2024-02-03", "2025-01-04", "2025-02-06", "2025-02-09"],
                "Amount": [90.0, 120.0, 140.0, 20.0],
                "Category": ["A", "A", "A", "B"],
            }
        )
    )


def _optional_string(value: Any) -> str | None:
    if isinstance(value, str):
        text = value.strip()
        return text if text else None
    return None


def _optional_int(value: Any) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    return None


def _optional_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes"}:
            return True
        if normalized in {"false", "0", "no"}:
            return False
    return None


def _optional_filter_list(value: Any) -> list[dict[str, str]] | None:
    if not isinstance(value, list):
        return None
    output: list[dict[str, str]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        column = str(item.get("column") or "").strip()
        filter_value = str(item.get("value") or "").strip()
        if not column:
            continue
        output.append({"column": column, "value": filter_value})
    return output or None


def _optional_context(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    return dict(value)


def _normalize_value_filters(value: Any) -> list[dict[str, str]]:
    if not isinstance(value, list):
        return []
    output: list[dict[str, str]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        column = str(item.get("column") or "").strip()
        filter_value = str(item.get("value") or "").strip()
        if not column:
            continue
        output.append({"column": column, "value": filter_value})
    return output
