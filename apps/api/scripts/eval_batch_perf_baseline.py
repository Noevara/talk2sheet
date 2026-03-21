from __future__ import annotations

import argparse
import json
import statistics
import sys
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

API_ROOT = Path(__file__).resolve().parents[1]
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

import app.services.spreadsheet.service as service_module
from app.schemas import ExecutionDisclosure
from app.services.spreadsheet.analysis.types import AnalysisPayload


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Evaluate synthetic batch analysis performance baseline.",
    )
    parser.add_argument(
        "--sheets",
        type=int,
        default=6,
        help="Number of sheets in synthetic workbook. Default: 6",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=64,
        help="Rows per sheet in synthetic workbook. Default: 64",
    )
    parser.add_argument(
        "--repeats",
        type=int,
        default=3,
        help="Benchmark repeats for each parallelism setting. Default: 3",
    )
    parser.add_argument(
        "--sleep-ms",
        type=int,
        default=40,
        help="Synthetic per-sheet analysis sleep in ms. Default: 40",
    )
    parser.add_argument(
        "--parallel",
        type=int,
        default=3,
        help="Parallel worker setting to benchmark against sequential baseline. Default: 3",
    )
    parser.add_argument(
        "--min-speedup",
        type=float,
        default=1.25,
        help="Minimum required median speedup (sequential / parallel). Default: 1.25",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=API_ROOT / ".artifacts" / "batch_perf_baseline_report.json",
        help=f"Path to benchmark report JSON. Default: {API_ROOT / '.artifacts' / 'batch_perf_baseline_report.json'}",
    )
    parser.add_argument(
        "--failure-snapshot",
        type=Path,
        default=API_ROOT / ".artifacts" / "batch_perf_baseline_failure_snapshot.json",
        help=(
            "Path to write failure snapshot when threshold is not met. "
            f"Default: {API_ROOT / '.artifacts' / 'batch_perf_baseline_failure_snapshot.json'}"
        ),
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print JSON summary output.",
    )
    return parser


def _write_synthetic_workbook(path: Path, *, sheets: int, rows: int) -> None:
    rows = max(8, rows)
    with pd.ExcelWriter(path) as writer:
        for index in range(1, sheets + 1):
            df = pd.DataFrame(
                {
                    "Date": [f"2026-01-{(day % 28) + 1:02d}" for day in range(rows)],
                    "Value": [index * 100 + day for day in range(rows)],
                    "Category": [f"C{day % 4}" for day in range(rows)],
                }
            )
            df.to_excel(writer, sheet_name=f"Sheet{index}", index=False)


def _fake_analysis_payload(sheet_index: int) -> AnalysisPayload:
    return AnalysisPayload(
        mode="text",
        answer=f"sheet {sheet_index} done",
        analysis_text=f"sheet {sheet_index} done",
        pipeline={
            "status": "ok",
            "planner": {"intent": f"sheet_{sheet_index}"},
            "result_row_count": 1,
            "result_columns": ["value"],
        },
        execution_disclosure=ExecutionDisclosure(
            data_scope="exact_full_table",
            exact_used=True,
            scope_text="full table",
            scope_warning="",
            fallback_reason="",
            fallback_reason_code="",
        ),
    )


def _run_once(
    *,
    workbook_path: Path,
    file_id: str,
    question: str,
    sheet_indexes: list[int],
    sleep_ms: int,
    parallelism: int,
) -> float:
    def _fake_analyze(*_args, **kwargs):
        time.sleep(max(0, sleep_ms) / 1000.0)
        sheet_index = int(kwargs.get("source_sheet_index") or 0)
        return _fake_analysis_payload(sheet_index)

    with patch.object(service_module, "analyze", _fake_analyze):
        with patch.object(
            service_module,
            "get_settings",
            lambda: SimpleNamespace(max_analysis_rows=50000, batch_max_parallel=parallelism),
        ):
            started = time.perf_counter()
            response = service_module.run_batch_workbook_analysis(
                path=workbook_path,
                file_id=file_id,
                question=question,
                mode="text",
                sheet_indexes=sheet_indexes,
                locale="en",
                request_id=f"req-perf-p{parallelism}",
            )
            elapsed_ms = (time.perf_counter() - started) * 1000.0

    # Keep a strict correctness check in baseline runs.
    if [item.sheet_index for item in response.batch_results] != sheet_indexes:
        raise RuntimeError("Batch result order mismatch in performance baseline run.")
    if response.summary.total != len(sheet_indexes) or response.summary.failed != 0:
        raise RuntimeError("Unexpected failed batch result in performance baseline run.")
    return elapsed_ms


def _evaluate_perf_baseline(
    *,
    workbook_path: Path,
    sheet_indexes: list[int],
    repeats: int,
    sleep_ms: int,
    parallelism: int,
) -> dict[str, object]:
    sequential_timings = [
        _run_once(
            workbook_path=workbook_path,
            file_id="file-perf-seq",
            question="Synthetic perf baseline",
            sheet_indexes=sheet_indexes,
            sleep_ms=sleep_ms,
            parallelism=1,
        )
        for _ in range(repeats)
    ]
    parallel_timings = [
        _run_once(
            workbook_path=workbook_path,
            file_id="file-perf-parallel",
            question="Synthetic perf baseline",
            sheet_indexes=sheet_indexes,
            sleep_ms=sleep_ms,
            parallelism=parallelism,
        )
        for _ in range(repeats)
    ]

    seq_median = float(statistics.median(sequential_timings))
    par_median = float(statistics.median(parallel_timings))
    speedup = seq_median / par_median if par_median > 1e-9 else 0.0
    return {
        "sheets": len(sheet_indexes),
        "repeats": repeats,
        "sleep_ms_per_sheet": sleep_ms,
        "parallelism": parallelism,
        "sequential_ms": {
            "samples": [round(item, 3) for item in sequential_timings],
            "median": round(seq_median, 3),
        },
        "parallel_ms": {
            "samples": [round(item, 3) for item in parallel_timings],
            "median": round(par_median, 3),
        },
        "speedup_median": round(speedup, 3),
    }


def main() -> None:
    args = _build_arg_parser().parse_args()
    sheets = max(2, int(args.sheets))
    repeats = max(1, int(args.repeats))
    parallelism = max(1, int(args.parallel))
    sheet_indexes = list(range(1, sheets + 1))

    with tempfile.TemporaryDirectory(prefix="talk2sheet-batch-perf-") as tmp_dir:
        workbook_path = Path(tmp_dir) / "batch_perf_baseline.xlsx"
        _write_synthetic_workbook(workbook_path, sheets=sheets, rows=max(8, int(args.rows)))
        result = _evaluate_perf_baseline(
            workbook_path=workbook_path,
            sheet_indexes=sheet_indexes,
            repeats=repeats,
            sleep_ms=max(0, int(args.sleep_ms)),
            parallelism=parallelism,
        )

    min_speedup = float(args.min_speedup)
    speedup = float(result.get("speedup_median") or 0.0)
    passed = speedup >= min_speedup
    summary = {
        "passed": passed,
        "min_speedup": min_speedup,
        **result,
    }

    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    if args.json:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    else:
        print(
            "Batch perf baseline: "
            f"sequential_median={summary['sequential_ms']['median']}ms "
            f"parallel_median={summary['parallel_ms']['median']}ms "
            f"speedup={speedup:.3f}x "
            f"(min={min_speedup:.3f}x)"
        )
        print(f"Report written to: {args.report}")

    if not passed:
        args.failure_snapshot.parent.mkdir(parents=True, exist_ok=True)
        args.failure_snapshot.write_text(
            json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        print(f"Failure snapshot written to: {args.failure_snapshot}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
