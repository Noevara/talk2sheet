from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

API_ROOT = Path(__file__).resolve().parents[1]
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from app.services.spreadsheet.planning.join_beta_signals import evaluate_join_beta_request


DEFAULT_JOIN_BETA_CASES_PATH = API_ROOT / "tests" / "fixtures" / "join_beta_cases.v0.3.3.json"


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Evaluate Join Beta signal regression cases (v0.3.3 corpus).",
    )
    parser.add_argument(
        "--cases",
        type=Path,
        default=DEFAULT_JOIN_BETA_CASES_PATH,
        help=f"Path to join beta regression cases JSON. Default: {DEFAULT_JOIN_BETA_CASES_PATH}",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print JSON summary output.",
    )
    parser.add_argument(
        "--failure-snapshot",
        type=Path,
        default=API_ROOT / ".artifacts" / "join_beta_failure_snapshot.json",
        help=(
            "Path to write failure snapshot JSON when any case fails. "
            f"Default: {API_ROOT / '.artifacts' / 'join_beta_failure_snapshot.json'}"
        ),
    )
    return parser


def _load_cases(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError(f"Expected a JSON array in {path}")
    cases: list[dict[str, Any]] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        cases.append(item)
    return cases


def _normalize_reasons(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    reasons: list[str] = []
    for item in value:
        reason = str(item or "").strip()
        if reason:
            reasons.append(reason)
    return sorted(reasons)


def _evaluate_case(case: dict[str, Any]) -> dict[str, Any]:
    case_id = str(case.get("id") or "").strip() or "unknown"
    chat_text = str(case.get("chat_text") or "")
    expected = case.get("expected")
    if not isinstance(expected, dict):
        raise ValueError(f"Case '{case_id}' is missing 'expected' object.")

    actual = evaluate_join_beta_request(chat_text)
    failures: list[dict[str, Any]] = []
    for field in ("is_join_request", "eligible", "join_key", "join_type", "is_aggregate_query"):
        if field not in expected:
            continue
        expected_value = expected.get(field)
        actual_value = actual.get(field)
        if field in {"is_join_request", "eligible", "is_aggregate_query"}:
            expected_value = bool(expected_value)
            actual_value = bool(actual_value)
        else:
            expected_value = str(expected_value or "")
            actual_value = str(actual_value or "")
        if actual_value != expected_value:
            failures.append(
                {
                    "field": field,
                    "expected": expected_value,
                    "actual": actual_value,
                }
            )

    if "reasons" in expected:
        expected_reasons = _normalize_reasons(expected.get("reasons"))
        actual_reasons = _normalize_reasons(actual.get("reasons"))
        if actual_reasons != expected_reasons:
            failures.append(
                {
                    "field": "reasons",
                    "expected": expected_reasons,
                    "actual": actual_reasons,
                }
            )

    return {
        "id": case_id,
        "chat_text": chat_text,
        "passed": len(failures) == 0,
        "actual": actual,
        "expected": expected,
        "failures": failures,
    }


def _print_human_summary(summary: dict[str, Any], failures: list[dict[str, Any]]) -> None:
    total = int(summary.get("total") or 0)
    passed = int(summary.get("passed") or 0)
    failed = int(summary.get("failed") or 0)
    pass_rate = float(summary.get("pass_rate") or 0.0)
    print(f"Join beta regression: total={total} passed={passed} failed={failed} pass_rate={pass_rate:.1%}")
    if not failures:
        return
    print("Failed cases:")
    for item in failures:
        print(f"  - {item['id']}: {item['chat_text']}")
        for mismatch in item.get("failures") or []:
            print(
                f"      * {mismatch.get('field')}: expected={mismatch.get('expected')} actual={mismatch.get('actual')}"
            )


def main() -> None:
    args = _build_arg_parser().parse_args()
    cases = _load_cases(args.cases)
    results = [_evaluate_case(case) for case in cases]
    failures = [item for item in results if not bool(item.get("passed"))]
    summary = {
        "total": len(results),
        "passed": len(results) - len(failures),
        "failed": len(failures),
        "pass_rate": ((len(results) - len(failures)) / len(results)) if results else 1.0,
        "failures": failures,
    }

    if args.json:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    else:
        _print_human_summary(summary, failures)

    if failures:
        args.failure_snapshot.parent.mkdir(parents=True, exist_ok=True)
        args.failure_snapshot.write_text(
            json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        print(f"Failure snapshot written to: {args.failure_snapshot}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
