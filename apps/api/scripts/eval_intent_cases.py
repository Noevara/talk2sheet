from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

API_ROOT = Path(__file__).resolve().parents[1]
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from app.services.spreadsheet.planning.intent_regression import (
    DEFAULT_INTENT_CASES_PATH,
    build_intent_regression_failure_snapshot,
    evaluate_intent_regression_cases,
    load_intent_regression_cases,
    summarize_intent_regression_results,
)


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Evaluate planner intent regression cases (v0.3.0 corpus).",
    )
    parser.add_argument(
        "--cases",
        type=Path,
        default=DEFAULT_INTENT_CASES_PATH,
        help=f"Path to intent regression cases JSON. Default: {DEFAULT_INTENT_CASES_PATH}",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print JSON summary output.",
    )
    parser.add_argument(
        "--failure-snapshot",
        type=Path,
        default=API_ROOT / ".artifacts" / "intent_regression_failure_snapshot.json",
        help=(
            "Path to write failure snapshot JSON when any case fails. "
            f"Default: {API_ROOT / '.artifacts' / 'intent_regression_failure_snapshot.json'}"
        ),
    )
    return parser


def _print_human_summary(summary: dict, results: list) -> None:
    total = int(summary.get("total") or 0)
    passed = int(summary.get("passed") or 0)
    failed = int(summary.get("failed") or 0)
    pass_rate = float(summary.get("pass_rate") or 0.0)
    print(f"Intent regression: total={total} passed={passed} failed={failed} pass_rate={pass_rate:.1%}")
    categories = summary.get("categories") or {}
    if isinstance(categories, dict) and categories:
        print("Category breakdown:")
        for category, stats in sorted(categories.items()):
            if not isinstance(stats, dict):
                continue
            print(
                f"  - {category}: "
                f"{int(stats.get('passed') or 0)}/{int(stats.get('total') or 0)} "
                f"(failed={int(stats.get('failed') or 0)})"
            )
    scenarios = summary.get("scenarios") or {}
    if isinstance(scenarios, dict) and scenarios:
        print("Scenario breakdown:")
        for scenario, stats in sorted(scenarios.items()):
            if not isinstance(stats, dict):
                continue
            print(
                f"  - {scenario}: "
                f"{int(stats.get('passed') or 0)}/{int(stats.get('total') or 0)} "
                f"(failed={int(stats.get('failed') or 0)})"
            )

    failures = [result for result in results if not result.passed]
    if failures:
        print("Failed cases:")
        for result in failures:
            print(f"  - {result.case.id} ({result.case.chat_text})")
            for failure in result.failures:
                print(f"      * {failure}")
            print(
                f"      actual: intent={result.draft.intent}, "
                f"mode={result.draft.mode}, planner_meta={result.draft.planner_meta}"
            )


def main() -> None:
    args = _build_arg_parser().parse_args()
    cases = load_intent_regression_cases(args.cases)
    results = evaluate_intent_regression_cases(cases)
    summary = summarize_intent_regression_results(results)
    failure_snapshot = build_intent_regression_failure_snapshot(results)

    if args.json:
        print(
            json.dumps(
                {
                    **summary,
                    "failures": failure_snapshot,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    else:
        _print_human_summary(summary, results)

    if int(summary.get("failed") or 0) > 0:
        args.failure_snapshot.parent.mkdir(parents=True, exist_ok=True)
        args.failure_snapshot.write_text(
            json.dumps(
                {
                    **summary,
                    "failures": failure_snapshot,
                },
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        print(f"Failure snapshot written to: {args.failure_snapshot}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
