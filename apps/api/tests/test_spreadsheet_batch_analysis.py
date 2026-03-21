from __future__ import annotations

import json
from pathlib import Path
import time
from types import SimpleNamespace

import pandas as pd
import pytest

from app.schemas import ExecutionDisclosure
from app.services.spreadsheet.analysis.types import AnalysisPayload
from app.services.spreadsheet.service import run_batch_workbook_analysis, stream_batch_workbook_analysis


def _write_workbook(path: Path) -> None:
    with pd.ExcelWriter(path) as writer:
        pd.DataFrame({"Date": ["2026-01-01"], "Amount": [100]}).to_excel(writer, sheet_name="Sales", index=False)
        pd.DataFrame({"User": ["alice"], "Signup": ["2026-02-01"]}).to_excel(writer, sheet_name="Users", index=False)
        pd.DataFrame({"Campaign": ["spring"], "Clicks": [12]}).to_excel(writer, sheet_name="Marketing", index=False)


def _fake_success_result(*, intent: str = "row_count") -> AnalysisPayload:
    return AnalysisPayload(
        mode="text",
        answer="ok",
        analysis_text="ok",
        pipeline={
            "status": "ok",
            "planner": {"intent": intent},
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


def test_batch_analysis_returns_success_results_for_selected_sheets(tmp_path, monkeypatch) -> None:
    import app.services.spreadsheet.service as service_module

    workbook_path = tmp_path / "batch_success.xlsx"
    _write_workbook(workbook_path)

    def _fake_analyze(*_args, **kwargs):
        return _fake_success_result(intent="row_count")

    monkeypatch.setattr(service_module, "analyze", _fake_analyze)

    response = run_batch_workbook_analysis(
        path=workbook_path,
        file_id="file-batch-success",
        question="How many rows?",
        mode="text",
        sheet_indexes=[1, 2],
        locale="en",
        request_id="req-batch-success",
    )

    assert response.request_id == "req-batch-success"
    assert response.summary.total == 2
    assert response.summary.succeeded == 2
    assert response.summary.failed == 0
    assert response.sheet_indexes == [1, 2]
    assert [item.status for item in response.batch_results] == ["success", "success"]
    assert response.batch_results[0].pipeline["intent"] == "row_count"


def test_batch_analysis_isolates_sheet_failures(tmp_path, monkeypatch) -> None:
    import app.services.spreadsheet.service as service_module

    workbook_path = tmp_path / "batch_partial_fail.xlsx"
    _write_workbook(workbook_path)

    def _fake_analyze(*_args, **kwargs):
        if int(kwargs.get("source_sheet_index") or 0) == 2:
            raise RuntimeError("forced failure")
        return _fake_success_result(intent="ranking")

    monkeypatch.setattr(service_module, "analyze", _fake_analyze)

    response = run_batch_workbook_analysis(
        path=workbook_path,
        file_id="file-batch-partial-fail",
        question="Show top values",
        mode="auto",
        sheet_indexes=[1, 2, 3],
        locale="en",
        request_id="req-batch-partial-fail",
    )

    assert response.summary.total == 3
    assert response.summary.succeeded == 2
    assert response.summary.failed == 1

    status_map = {item.sheet_index: item.status for item in response.batch_results}
    assert status_map[1] == "success"
    assert status_map[2] == "failed"
    assert status_map[3] == "success"

    failed_item = next(item for item in response.batch_results if item.sheet_index == 2)
    assert failed_item.reason_code == "analysis_exception"
    assert failed_item.error


def test_batch_analysis_returns_full_failed_summary_when_all_sheets_fail(tmp_path, monkeypatch) -> None:
    import app.services.spreadsheet.service as service_module

    workbook_path = tmp_path / "batch_all_failed.xlsx"
    _write_workbook(workbook_path)

    def _fake_analyze(*_args, **_kwargs):
        raise RuntimeError("forced failure")

    monkeypatch.setattr(service_module, "analyze", _fake_analyze)

    response = run_batch_workbook_analysis(
        path=workbook_path,
        file_id="file-batch-all-failed",
        question="Show top values",
        mode="text",
        sheet_indexes=[1, 2, 3],
        locale="en",
        request_id="req-batch-all-failed",
    )

    assert response.summary.total == 3
    assert response.summary.succeeded == 0
    assert response.summary.failed == 3
    assert [item.status for item in response.batch_results] == ["failed", "failed", "failed"]
    assert all(item.reason_code == "analysis_exception" for item in response.batch_results)


def test_batch_analysis_uses_all_sheets_when_sheet_indexes_not_provided(tmp_path, monkeypatch) -> None:
    import app.services.spreadsheet.service as service_module

    workbook_path = tmp_path / "batch_all_sheets.xlsx"
    _write_workbook(workbook_path)

    monkeypatch.setattr(service_module, "analyze", lambda *_args, **_kwargs: _fake_success_result(intent="detail_rows"))

    response = run_batch_workbook_analysis(
        path=workbook_path,
        file_id="file-batch-all",
        question="Give detail rows",
        mode="text",
        sheet_indexes=[],
        locale="en",
        request_id="req-batch-all",
    )

    assert response.summary.total == 3
    assert response.sheet_indexes == [1, 2, 3]


def test_batch_analysis_keeps_result_order_when_parallel_enabled(tmp_path, monkeypatch) -> None:
    import app.services.spreadsheet.service as service_module

    workbook_path = tmp_path / "batch_parallel_order.xlsx"
    _write_workbook(workbook_path)

    monkeypatch.setattr(
        service_module,
        "get_settings",
        lambda: SimpleNamespace(max_analysis_rows=50000, batch_max_parallel=3),
    )

    def _fake_analyze(*_args, **kwargs):
        sheet_index = int(kwargs.get("source_sheet_index") or 0)
        if sheet_index == 1:
            time.sleep(0.05)
        return _fake_success_result(intent=f"sheet_{sheet_index}")

    monkeypatch.setattr(service_module, "analyze", _fake_analyze)

    response = run_batch_workbook_analysis(
        path=workbook_path,
        file_id="file-batch-parallel-order",
        question="How many rows?",
        mode="text",
        sheet_indexes=[1, 2, 3],
        locale="en",
        request_id="req-batch-parallel-order",
    )

    assert [item.sheet_index for item in response.batch_results] == [1, 2, 3]
    assert [item.pipeline["intent"] for item in response.batch_results] == ["sheet_1", "sheet_2", "sheet_3"]


@pytest.mark.asyncio
async def test_batch_stream_emits_progress_and_final_result(tmp_path, monkeypatch) -> None:
    import app.services.spreadsheet.service as service_module

    workbook_path = tmp_path / "batch_stream.xlsx"
    _write_workbook(workbook_path)

    monkeypatch.setattr(service_module, "analyze", lambda *_args, **_kwargs: _fake_success_result(intent="trend"))

    events: list[dict[str, object]] = []
    async for chunk in stream_batch_workbook_analysis(
        path=workbook_path,
        file_id="file-batch-stream",
        question="Show trend",
        mode="text",
        sheet_indexes=[1, 2],
        locale="en",
        request_id="req-batch-stream",
    ):
        payload = str(chunk).strip()
        if not payload.startswith("data:"):
            continue
        events.append(json.loads(payload[5:].strip()))

    assert events
    assert events[0]["type"] == "batch_progress"
    assert events[0]["done"] == 0
    assert events[0]["total"] == 2
    assert any(event.get("type") == "batch_progress" and event.get("done") == 1 for event in events)
    assert any(event.get("type") == "batch_progress" and event.get("done") == 2 for event in events)

    final_event = next(event for event in events if event.get("type") == "batch_result")
    assert final_event["request_id"] == "req-batch-stream"
    assert final_event["summary"]["total"] == 2
    assert final_event["summary"]["succeeded"] == 2
    assert final_event["summary"]["failed"] == 0
    assert len(final_event["batch_results"]) == 2


@pytest.mark.asyncio
async def test_batch_stream_isolates_failed_sheet_and_keeps_running(tmp_path, monkeypatch) -> None:
    import app.services.spreadsheet.service as service_module

    workbook_path = tmp_path / "batch_stream_partial_fail.xlsx"
    _write_workbook(workbook_path)

    def _fake_analyze(*_args, **kwargs):
        if int(kwargs.get("source_sheet_index") or 0) == 2:
            raise RuntimeError("forced stream failure")
        return _fake_success_result(intent="ranking")

    monkeypatch.setattr(service_module, "analyze", _fake_analyze)

    events: list[dict[str, object]] = []
    async for chunk in stream_batch_workbook_analysis(
        path=workbook_path,
        file_id="file-batch-stream-fail",
        question="Show top values",
        mode="auto",
        sheet_indexes=[1, 2, 3],
        locale="en",
        request_id="req-batch-stream-fail",
    ):
        payload = str(chunk).strip()
        if not payload.startswith("data:"):
            continue
        events.append(json.loads(payload[5:].strip()))

    final_event = next(event for event in events if event.get("type") == "batch_result")
    assert final_event["summary"]["total"] == 3
    assert final_event["summary"]["succeeded"] == 2
    assert final_event["summary"]["failed"] == 1

    results = final_event["batch_results"]
    status_by_sheet = {item["sheet_index"]: item["status"] for item in results}
    assert status_by_sheet == {1: "success", 2: "failed", 3: "success"}


@pytest.mark.asyncio
async def test_batch_stream_keeps_final_result_order_when_parallel_enabled(tmp_path, monkeypatch) -> None:
    import app.services.spreadsheet.service as service_module

    workbook_path = tmp_path / "batch_stream_parallel_order.xlsx"
    _write_workbook(workbook_path)

    monkeypatch.setattr(
        service_module,
        "get_settings",
        lambda: SimpleNamespace(max_analysis_rows=50000, batch_max_parallel=3),
    )

    def _fake_analyze(*_args, **kwargs):
        sheet_index = int(kwargs.get("source_sheet_index") or 0)
        if sheet_index == 1:
            time.sleep(0.05)
        return _fake_success_result(intent=f"sheet_{sheet_index}")

    monkeypatch.setattr(service_module, "analyze", _fake_analyze)

    events: list[dict[str, object]] = []
    async for chunk in stream_batch_workbook_analysis(
        path=workbook_path,
        file_id="file-batch-stream-parallel-order",
        question="Show trend",
        mode="text",
        sheet_indexes=[1, 2, 3],
        locale="en",
        request_id="req-batch-stream-parallel-order",
    ):
        payload = str(chunk).strip()
        if not payload.startswith("data:"):
            continue
        events.append(json.loads(payload[5:].strip()))

    final_event = next(event for event in events if event.get("type") == "batch_result")
    assert [item["sheet_index"] for item in final_event["batch_results"]] == [1, 2, 3]
