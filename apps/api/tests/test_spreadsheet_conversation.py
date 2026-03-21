from __future__ import annotations

import json
import time
from pathlib import Path

import pandas as pd
import pytest

from app.services.spreadsheet.conversation.conversation_memory import ConversationStore, conversation_store
from app.services.spreadsheet.core.schema import Metric, SelectionPlan, TransformPlan
from app.services.spreadsheet.planning.intent_models import AnalysisIntent
from app.services.spreadsheet.planning.planner_types import PlanDraft
from app.services.spreadsheet.service import stream_spreadsheet_chat


class _RecordingPlanner:
    name = "recording-planner"

    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def plan(self, df, *, chat_text: str, requested_mode: str, followup_context=None):
        self.calls.append(
            {
                "chat_text": chat_text,
                "requested_mode": requested_mode,
                "followup_context": followup_context,
            }
        )
        return PlanDraft(
            mode="text",
            intent="row_count",
            selection_plan=SelectionPlan(),
            transform_plan=TransformPlan(metrics=[Metric(agg="count_rows", as_name="row_count")]),
            analysis_intent=AnalysisIntent(
                kind="row_count",
                legacy_intent="row_count",
                target_metric="row_count",
                answer_expectation="single_value",
            ),
            planner_meta={"planner": self.name},
        )


def _write_csv(path: Path) -> None:
    pd.DataFrame(
        {
            "Date": ["2025-01-01", "2025-01-10", "2025-02-03"],
            "Category": ["A", "B", "A"],
            "Amount": [100, 50, 80],
        }
    ).to_csv(path, index=False)


def _write_workbook(path: Path) -> None:
    with pd.ExcelWriter(path) as writer:
        pd.DataFrame(
            {
                "Date": ["2025-01-01", "2025-01-10"],
                "Amount": [100, 80],
                "Region": ["cn-sh", "cn-bj"],
            }
        ).to_excel(writer, sheet_name="Sales", index=False)
        pd.DataFrame(
            {
                "User Name": ["Alice", "Bob", "Carol"],
                "Email": ["a@test.com", "b@test.com", "c@test.com"],
                "Signup Date": ["2025-02-01", "2025-02-03", "2025-02-05"],
            }
        ).to_excel(writer, sheet_name="Users", index=False)


async def _collect_events(**kwargs) -> list[dict[str, object]]:
    events: list[dict[str, object]] = []
    async for chunk in stream_spreadsheet_chat(**kwargs):
        payload = str(chunk).strip()
        if not payload.startswith("data:"):
            continue
        events.append(json.loads(payload[5:].strip()))
    return events


@pytest.mark.asyncio
async def test_stream_chat_persists_conversation_and_passes_followup_context(tmp_path, monkeypatch) -> None:
    import app.services.spreadsheet.analysis as analysis_module

    conversation_store.clear()
    planner = _RecordingPlanner()
    monkeypatch.setattr(analysis_module, "get_default_planner", lambda: planner)

    csv_path = tmp_path / "sample.csv"
    _write_csv(csv_path)

    first_events = await _collect_events(
        path=csv_path,
        file_id="file-1",
        chat_text="How many rows are in this sheet?",
        mode="text",
        sheet_index=1,
        sheet_override=False,
        locale="en",
        conversation_id=None,
    )

    first_conversation_id = str(first_events[0]["conversation_id"])
    first_request_id = str(first_events[0]["request_id"])
    assert first_conversation_id
    assert first_request_id
    assert all(str(event["conversation_id"]) == first_conversation_id for event in first_events if "conversation_id" in event)
    assert all(str(event["request_id"]) == first_request_id for event in first_events if "request_id" in event)
    assert planner.calls[0]["followup_context"] is None
    assert isinstance(first_events[2].get("answer_segments"), dict)
    assert first_events[2]["answer_segments"]["conclusion"]
    assert "evidence" in first_events[2]["answer_segments"]
    assert first_events[1]["pipeline"]["observability"]["request_id"] == first_request_id
    assert first_events[1]["pipeline"]["observability"]["stage_timings_ms"]["planner_ms"] >= 0
    assert first_events[1]["pipeline"]["observability"]["total_ms"] >= 0
    assert first_events[1]["pipeline"]["observability"]["multi_sheet_detected"] == 0
    assert first_events[1]["pipeline"]["observability"]["clarification_sheet_count"] == 0
    assert first_events[1]["pipeline"]["observability"]["sheet_switch_count"] == 0
    assert first_events[1]["pipeline"]["task_steps"] == []
    assert first_events[1]["pipeline"]["current_step_id"] == ""

    second_events = await _collect_events(
        path=csv_path,
        file_id="file-1",
        chat_text="Continue with the same analysis but as a chart.",
        mode="auto",
        sheet_index=1,
        sheet_override=False,
        locale="en",
        conversation_id=first_conversation_id,
    )

    assert all(str(event["conversation_id"]) == first_conversation_id for event in second_events if "conversation_id" in event)
    assert all("request_id" in event for event in second_events)
    second_context = planner.calls[1]["followup_context"]
    assert isinstance(second_context, dict)
    assert second_context["conversation_id"] == first_conversation_id
    assert second_context["turn_count"] == 1
    assert second_context["is_followup"] is True
    assert second_context["last_mode"] == "text"
    assert second_context["last_pipeline_summary"]["intent"] == "row_count"
    assert second_context["last_pipeline_summary"]["analysis_intent"]["kind"] == "row_count"
    assert second_context["last_pipeline_summary"]["result_row_count"] == 1
    assert second_context["last_turn"]["question"] == "How many rows are in this sheet?"
    assert second_context["last_turn"]["intent"] == "row_count"
    assert second_context["last_turn"]["analysis_intent"]["kind"] == "row_count"
    assert isinstance(second_context.get("analysis_anchor"), dict)
    assert second_context["analysis_anchor"]["intent"] == "row_count"
    assert second_context["analysis_anchor"]["metric_agg"] == "count_rows"
    assert len(second_context["recent_pipeline_history"]) == 1
    assert second_context["recent_pipeline_history"][0]["pipeline_summary"]["intent"] == "row_count"
    assert second_context["recent_pipeline_history"][0]["pipeline_summary"]["analysis_intent"]["kind"] == "row_count"
    assert second_context["recent_pipeline_history"][0]["execution_disclosure"]["data_scope"] == "exact_full_table"
    assert second_events[1]["pipeline"]["analysis_anchor_reused"] is True
    assert isinstance(second_events[1]["pipeline"]["analysis_anchor"], dict)

    session = conversation_store.get_session(first_conversation_id)
    assert session is not None
    assert len(session.turns) == 2
    assert session.turns[0]["pipeline_summary"]["intent"] == "row_count"
    assert session.turns[1]["pipeline_summary"]["intent"] == "row_count"
    assert session.turns[0]["analysis_intent"]["kind"] == "row_count"


@pytest.mark.asyncio
async def test_stream_chat_passes_structured_clarification_resolution_into_followup_context(tmp_path, monkeypatch) -> None:
    import app.services.spreadsheet.analysis as analysis_module

    conversation_store.clear()
    planner = _RecordingPlanner()
    monkeypatch.setattr(analysis_module, "get_default_planner", lambda: planner)

    csv_path = tmp_path / "clarification_sample.csv"
    _write_csv(csv_path)

    first_events = await _collect_events(
        path=csv_path,
        file_id="file-clarification",
        chat_text="How many rows are in this sheet?",
        mode="text",
        sheet_index=1,
        sheet_override=False,
        locale="en",
        conversation_id=None,
    )
    conversation_id = str(first_events[0]["conversation_id"])

    await _collect_events(
        path=csv_path,
        file_id="file-clarification",
        chat_text="How many rows are in this sheet?",
        mode="text",
        sheet_index=1,
        sheet_override=False,
        locale="en",
        conversation_id=conversation_id,
        clarification_resolution={
            "kind": "column_resolution",
            "source_field": "Name",
            "selected_value": "Category",
        },
    )

    second_context = planner.calls[1]["followup_context"]
    assert isinstance(second_context, dict)
    assert second_context["is_followup"] is True
    assert second_context["clarification_resolution"]["selected_value"] == "Category"
    assert second_context["clarification_resolution"]["source_field"] == "Name"


@pytest.mark.asyncio
async def test_stream_chat_auto_routes_to_matching_sheet_in_workbook(tmp_path, monkeypatch) -> None:
    import app.services.spreadsheet.analysis as analysis_module

    conversation_store.clear()
    planner = _RecordingPlanner()
    monkeypatch.setattr(analysis_module, "get_default_planner", lambda: planner)

    workbook_path = tmp_path / "workbook.xlsx"
    _write_workbook(workbook_path)

    events = await _collect_events(
        path=workbook_path,
        file_id="file-workbook-1",
        chat_text="How many email records are there?",
        mode="text",
        sheet_index=1,
        sheet_override=False,
        locale="en",
        conversation_id=None,
    )

    assert events[0]["meta"]["resolved_sheet_index"] == 2
    assert events[0]["meta"]["resolved_sheet_name"] == "Users"
    assert events[0]["meta"]["sheet_routing"]["matched_by"] == "auto_routing"
    assert "Auto-routed" in events[0]["meta"]["sheet_routing"]["explanation"]
    assert events[1]["pipeline"]["source_sheet_index"] == 2
    assert events[1]["pipeline"]["source_sheet_name"] == "Users"
    assert events[1]["pipeline"]["sheet_routing"]["reason"] == "auto_routed_by_sheet_profile"


@pytest.mark.asyncio
async def test_stream_chat_returns_multi_sheet_clarification_for_cross_sheet_question(tmp_path, monkeypatch) -> None:
    import app.services.spreadsheet.analysis as analysis_module

    conversation_store.clear()
    planner = _RecordingPlanner()
    monkeypatch.setattr(analysis_module, "get_default_planner", lambda: planner)

    workbook_path = tmp_path / "workbook_multisheet.xlsx"
    _write_workbook(workbook_path)

    events = await _collect_events(
        path=workbook_path,
        file_id="file-workbook-multi",
        chat_text="Join Sales and Users by email and show conversion.",
        mode="text",
        sheet_index=1,
        sheet_override=False,
        locale="en",
        conversation_id=None,
    )

    assert events[0]["meta"]["sheet_routing"]["status"] == "clarification"
    assert events[0]["meta"]["sheet_routing"]["reason"] == "multi_sheet_needs_decomposition"
    assert events[0]["meta"]["sheet_routing"]["boundary_status"] == "multi_sheet_out_of_scope"
    assert "choose one sheet first" in str(events[0]["meta"]["sheet_routing"]["explanation"]).lower()
    assert "decomposition_hint" in events[0]["meta"]["sheet_routing"]
    assert "Suggested decomposition" in events[0]["meta"]["sheet_routing"]["decomposition_hint"]
    assert events[0]["meta"]["observability"]["sheet_routing"]["multi_sheet_detected"] == 1
    assert events[1]["pipeline"]["clarification_stage"] == "sheet_routing"
    assert events[1]["pipeline"]["sheet_routing"]["matched_by"] == "multi_sheet_boundary"
    assert len(events[1]["pipeline"]["clarification"]["options"]) == 2
    assert events[1]["pipeline"]["observability"]["multi_sheet_detected"] == 1
    assert events[1]["pipeline"]["observability"]["clarification_sheet_count"] == 2
    assert events[1]["pipeline"]["observability"]["sheet_switch_count"] == 0
    assert events[1]["pipeline"]["observability"]["multi_sheet_failure_reason"] == "cross_sheet_join_not_supported"
    assert events[1]["pipeline"]["observability"]["multi_sheet_top_failure_reasons"]["cross_sheet_join_not_supported"] >= 1
    assert len(events[1]["pipeline"]["task_steps"]) == 2
    assert events[1]["pipeline"]["task_steps"][0]["status"] == "current"
    assert events[1]["pipeline"]["current_step_id"] == "sheet-1"
    assert "multiple sheets" in str(events[2]["answer"]).lower()


@pytest.mark.asyncio
async def test_stream_chat_followup_inherits_auto_routed_sheet(tmp_path, monkeypatch) -> None:
    import app.services.spreadsheet.analysis as analysis_module

    conversation_store.clear()
    planner = _RecordingPlanner()
    monkeypatch.setattr(analysis_module, "get_default_planner", lambda: planner)

    workbook_path = tmp_path / "workbook_followup.xlsx"
    _write_workbook(workbook_path)

    first_events = await _collect_events(
        path=workbook_path,
        file_id="file-workbook-2",
        chat_text="How many email records are there?",
        mode="text",
        sheet_index=1,
        sheet_override=False,
        locale="en",
        conversation_id=None,
    )
    conversation_id = str(first_events[0]["conversation_id"])

    second_events = await _collect_events(
        path=workbook_path,
        file_id="file-workbook-2",
        chat_text="Continue with the same analysis but as a chart.",
        mode="auto",
        sheet_index=1,
        sheet_override=False,
        locale="en",
        conversation_id=conversation_id,
    )

    second_context = planner.calls[1]["followup_context"]
    assert isinstance(second_context, dict)
    assert second_context["last_sheet_index"] == 2
    assert second_context["last_sheet_name"] == "Users"
    assert second_context["last_turn"]["sheet_name"] == "Users"
    assert isinstance(second_context["visited_sheets"], list)
    assert second_context["visited_sheets"][0]["sheet_index"] == 2
    assert second_events[0]["meta"]["resolved_sheet_index"] == 2
    assert second_events[1]["pipeline"]["source_sheet_index"] == 2


@pytest.mark.asyncio
async def test_stream_chat_followup_can_switch_to_another_sheet_sequentially(tmp_path, monkeypatch) -> None:
    import app.services.spreadsheet.analysis as analysis_module

    conversation_store.clear()
    planner = _RecordingPlanner()
    monkeypatch.setattr(analysis_module, "get_default_planner", lambda: planner)

    workbook_path = tmp_path / "workbook_switch.xlsx"
    _write_workbook(workbook_path)

    first_events = await _collect_events(
        path=workbook_path,
        file_id="file-workbook-switch",
        chat_text="Show total amount in Sales.",
        mode="text",
        sheet_index=1,
        sheet_override=False,
        locale="en",
        conversation_id=None,
    )
    conversation_id = str(first_events[0]["conversation_id"])

    second_events = await _collect_events(
        path=workbook_path,
        file_id="file-workbook-switch",
        chat_text="Continue on another sheet.",
        mode="auto",
        sheet_index=1,
        sheet_override=False,
        locale="en",
        conversation_id=conversation_id,
    )

    assert second_events[0]["meta"]["resolved_sheet_index"] == 2
    assert second_events[0]["meta"]["resolved_sheet_name"] == "Users"
    assert second_events[0]["meta"]["sheet_routing"]["reason"] == "followup_switch_to_another_sheet"
    assert second_events[0]["meta"]["sheet_sequence"]["switched_from_previous"] is True
    assert second_events[0]["meta"]["sheet_sequence"]["previous_sheet_index"] == 1
    assert len(second_events[0]["meta"]["task_steps"]) >= 2
    assert second_events[0]["meta"]["current_step_id"] == "sheet-2"
    assert second_events[1]["pipeline"]["sheet_sequence"]["last_sheet_switch_reason"] == "followup_switch_to_another_sheet"
    assert second_events[1]["pipeline"]["source_sheet_index"] == 2
    assert second_events[1]["pipeline"]["observability"]["sheet_switch_count"] == 1
    assert second_events[1]["pipeline"]["task_steps"][0]["status"] == "completed"
    assert second_events[1]["pipeline"]["task_steps"][1]["status"] == "current"
    assert second_events[1]["pipeline"]["step_comparison"]["previous_step"]["sheet_index"] == 1
    assert second_events[1]["pipeline"]["step_comparison"]["current_step"]["sheet_index"] == 2
    assert second_events[1]["pipeline"]["step_comparison"]["independent_scopes"] is True


@pytest.mark.asyncio
async def test_stream_chat_followup_action_continue_next_step_routes_to_pending_sheet(tmp_path, monkeypatch) -> None:
    import app.services.spreadsheet.analysis as analysis_module

    conversation_store.clear()
    planner = _RecordingPlanner()
    monkeypatch.setattr(analysis_module, "get_default_planner", lambda: planner)

    workbook_path = tmp_path / "workbook_continue_next_step.xlsx"
    _write_workbook(workbook_path)
    question = "Join Sales and Users by email and show conversion."

    clarification_events = await _collect_events(
        path=workbook_path,
        file_id="file-workbook-next-step",
        chat_text=question,
        mode="text",
        sheet_index=1,
        sheet_override=False,
        locale="en",
        conversation_id=None,
    )
    conversation_id = str(clarification_events[0]["conversation_id"])
    assert clarification_events[1]["pipeline"]["status"] == "clarification"

    resolved_events = await _collect_events(
        path=workbook_path,
        file_id="file-workbook-next-step",
        chat_text=question,
        mode="text",
        sheet_index=1,
        sheet_override=False,
        locale="en",
        conversation_id=conversation_id,
        clarification_resolution={
            "kind": "sheet_resolution",
            "selected_value": "Sales",
        },
    )
    assert resolved_events[0]["meta"]["resolved_sheet_index"] == 1
    assert resolved_events[1]["pipeline"]["current_step_id"] == "sheet-1"

    next_step_events = await _collect_events(
        path=workbook_path,
        file_id="file-workbook-next-step",
        chat_text=question,
        mode="text",
        sheet_index=1,
        sheet_override=False,
        locale="en",
        conversation_id=conversation_id,
        followup_action="continue_next_step",
    )

    assert next_step_events[0]["meta"]["resolved_sheet_index"] == 2
    assert next_step_events[0]["meta"]["resolved_sheet_name"] == "Users"
    assert next_step_events[0]["meta"]["followup_action"] == "continue_next_step"
    assert next_step_events[0]["meta"]["followup_action_target_sheet_index"] == 2
    assert next_step_events[1]["pipeline"]["sheet_routing"]["reason"] == "clarification_resolution"
    assert next_step_events[1]["pipeline"]["current_step_id"] == "sheet-2"
    assert next_step_events[1]["pipeline"]["task_steps"][0]["status"] == "completed"
    assert next_step_events[1]["pipeline"]["task_steps"][1]["status"] == "current"
    assert next_step_events[1]["pipeline"]["observability"]["followup_action"] == "continue_next_step"
    assert next_step_events[1]["pipeline"]["observability"]["followup_action_applied"] == 1
    assert next_step_events[1]["pipeline"]["observability"]["followup_action_target_sheet_index"] == 2
    assert next_step_events[1]["pipeline"]["observability"]["task_step_started_count"] == 1
    assert next_step_events[1]["pipeline"]["observability"]["task_step_completed_count"] == 1
    assert len(next_step_events[1]["pipeline"]["observability"]["task_step_events"]) == 2
    assert next_step_events[1]["pipeline"]["observability"]["task_step_events"][0]["event"] == "task_step_completed"
    assert next_step_events[1]["pipeline"]["observability"]["task_step_events"][0]["step_id"] == "sheet-1"
    assert next_step_events[1]["pipeline"]["observability"]["task_step_events"][1]["event"] == "task_step_started"
    assert next_step_events[1]["pipeline"]["observability"]["task_step_events"][1]["step_id"] == "sheet-2"
    assert next_step_events[1]["pipeline"]["step_comparison"]["previous_step"]["sheet_index"] == 1
    assert next_step_events[1]["pipeline"]["step_comparison"]["current_step"]["sheet_index"] == 2
    assert next_step_events[1]["pipeline"]["step_comparison"]["independent_scopes"] is True

    next_step_context = planner.calls[1]["followup_context"]
    assert isinstance(next_step_context, dict)
    assert next_step_context["followup_action"] == "continue_next_step"
    assert next_step_context["wants_sheet_switch"] is True
    assert next_step_context["current_sheet_index"] == 2


@pytest.mark.asyncio
async def test_stream_chat_logs_task_step_failed_with_request_and_step_id(tmp_path, monkeypatch) -> None:
    import app.services.spreadsheet.analysis as analysis_module
    import app.services.spreadsheet.service as service_module

    conversation_store.clear()
    planner = _RecordingPlanner()
    monkeypatch.setattr(analysis_module, "get_default_planner", lambda: planner)

    recorded_task_step_events: list[dict[str, object]] = []

    def _record_task_step_event(*_args, **kwargs):
        recorded_task_step_events.append(dict(kwargs))

    monkeypatch.setattr(service_module, "log_task_step_event", _record_task_step_event)

    workbook_path = tmp_path / "workbook_task_step_failed.xlsx"
    _write_workbook(workbook_path)
    question = "Join Sales and Users by email and show conversion."

    clarification_events = await _collect_events(
        path=workbook_path,
        file_id="file-task-step-failed",
        chat_text=question,
        mode="text",
        sheet_index=1,
        sheet_override=False,
        locale="en",
        conversation_id=None,
    )
    conversation_id = str(clarification_events[0]["conversation_id"])

    await _collect_events(
        path=workbook_path,
        file_id="file-task-step-failed",
        chat_text=question,
        mode="text",
        sheet_index=1,
        sheet_override=False,
        locale="en",
        conversation_id=conversation_id,
        clarification_resolution={
            "kind": "sheet_resolution",
            "selected_value": "Sales",
        },
    )

    def _failing_analyze(*_args, **_kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(service_module, "analyze", _failing_analyze)

    failed_events = await _collect_events(
        path=workbook_path,
        file_id="file-task-step-failed",
        chat_text=question,
        mode="text",
        sheet_index=1,
        sheet_override=False,
        locale="en",
        conversation_id=conversation_id,
        followup_action="continue_next_step",
    )

    failed_request_id = str(failed_events[0]["request_id"])
    emitted = [
        item
        for item in recorded_task_step_events
        if str(item.get("request_id") or "") == failed_request_id
    ]
    emitted_names = [str(item.get("event") or "") for item in emitted]

    assert "task_step_completed" in emitted_names
    assert "task_step_started" in emitted_names
    assert "task_step_failed" in emitted_names

    failed_log = next(item for item in emitted if str(item.get("event") or "") == "task_step_failed")
    assert failed_log["step_id"] == "sheet-2"
    assert failed_log["step_status"] == "failed"
    assert failed_log["followup_action"] == "continue_next_step"
    assert failed_log["error_type"] == "RuntimeError"


@pytest.mark.asyncio
async def test_stream_chat_followup_can_switch_back_to_previous_sheet(tmp_path, monkeypatch) -> None:
    import app.services.spreadsheet.analysis as analysis_module

    conversation_store.clear()
    planner = _RecordingPlanner()
    monkeypatch.setattr(analysis_module, "get_default_planner", lambda: planner)

    workbook_path = tmp_path / "workbook_switch_back.xlsx"
    _write_workbook(workbook_path)

    first_events = await _collect_events(
        path=workbook_path,
        file_id="file-workbook-switch-back",
        chat_text="Show total amount in Sales.",
        mode="text",
        sheet_index=1,
        sheet_override=False,
        locale="en",
        conversation_id=None,
    )
    conversation_id = str(first_events[0]["conversation_id"])

    await _collect_events(
        path=workbook_path,
        file_id="file-workbook-switch-back",
        chat_text="Continue on another sheet.",
        mode="auto",
        sheet_index=1,
        sheet_override=False,
        locale="en",
        conversation_id=conversation_id,
    )

    third_events = await _collect_events(
        path=workbook_path,
        file_id="file-workbook-switch-back",
        chat_text="Back to previous sheet and continue.",
        mode="auto",
        sheet_index=2,
        sheet_override=False,
        locale="en",
        conversation_id=conversation_id,
    )

    third_context = planner.calls[2]["followup_context"]
    assert isinstance(third_context, dict)
    assert third_context["sheet_reference_hint"] == "previous"
    assert third_context["previous_sheet_index"] == 1
    assert isinstance(third_context["recent_sheet_trajectory"], list)
    assert len(third_context["recent_sheet_trajectory"]) >= 2

    assert third_events[0]["meta"]["resolved_sheet_index"] == 1
    assert third_events[0]["meta"]["sheet_routing"]["reason"] == "followup_switch_to_previous_sheet"
    assert third_events[1]["pipeline"]["source_sheet_index"] == 1
    assert third_events[1]["pipeline"]["sheet_sequence"]["last_sheet_switch_reason"] == "followup_switch_to_previous_sheet"

@pytest.mark.asyncio
async def test_stream_chat_reuses_session_cached_dataframes(tmp_path, monkeypatch) -> None:
    import app.services.spreadsheet.analysis as analysis_module
    import app.services.spreadsheet.service as service_module
    from app.services.spreadsheet.pipeline import load_dataframe as pipeline_load_dataframe, load_full_dataframe as pipeline_load_full_dataframe

    conversation_store.clear()
    planner = _RecordingPlanner()
    monkeypatch.setattr(analysis_module, "get_default_planner", lambda: planner)

    csv_path = tmp_path / "cached_session_sample.csv"
    _write_csv(csv_path)

    counters = {"sampled": 0, "full": 0}

    def _counted_load_dataframe(*args, **kwargs):
        counters["sampled"] += 1
        return pipeline_load_dataframe(*args, **kwargs)

    def _counted_load_full_dataframe(*args, **kwargs):
        counters["full"] += 1
        return pipeline_load_full_dataframe(*args, **kwargs)

    monkeypatch.setattr(service_module, "load_dataframe", _counted_load_dataframe)
    monkeypatch.setattr(service_module, "load_full_dataframe", _counted_load_full_dataframe)

    first_events = await _collect_events(
        path=csv_path,
        file_id="file-cache",
        chat_text="How many rows are in this sheet?",
        mode="text",
        sheet_index=1,
        sheet_override=False,
        locale="en",
        conversation_id=None,
    )
    conversation_id = str(first_events[0]["conversation_id"])

    await _collect_events(
        path=csv_path,
        file_id="file-cache",
        chat_text="How many rows are in this sheet?",
        mode="text",
        sheet_index=1,
        sheet_override=False,
        locale="en",
        conversation_id=conversation_id,
    )

    assert counters["sampled"] == 1
    assert counters["full"] == 1


@pytest.mark.asyncio
async def test_stream_chat_returns_request_id_when_analysis_fails(tmp_path, monkeypatch) -> None:
    import app.services.spreadsheet.service as service_module

    conversation_store.clear()

    csv_path = tmp_path / "error_sample.csv"
    _write_csv(csv_path)

    def _failing_analyze(*args, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(service_module, "analyze", _failing_analyze)

    events = await _collect_events(
        path=csv_path,
        file_id="file-error",
        chat_text="How many rows are in this sheet?",
        mode="text",
        sheet_index=1,
        sheet_override=False,
        locale="en",
        conversation_id=None,
        request_id="req-test-error",
    )

    assert events[-2]["request_id"] == "req-test-error"
    assert "Request ID: req-test-error" in str(events[-2]["answer"])


@pytest.mark.asyncio
async def test_stream_chat_manual_sheet_override_beats_followup_inheritance(tmp_path, monkeypatch) -> None:
    import app.services.spreadsheet.analysis as analysis_module

    conversation_store.clear()
    planner = _RecordingPlanner()
    monkeypatch.setattr(analysis_module, "get_default_planner", lambda: planner)

    workbook_path = tmp_path / "workbook_manual_override.xlsx"
    _write_workbook(workbook_path)

    first_events = await _collect_events(
        path=workbook_path,
        file_id="file-workbook-override",
        chat_text="How many email records are there?",
        mode="text",
        sheet_index=1,
        sheet_override=False,
        locale="en",
        conversation_id=None,
    )
    conversation_id = str(first_events[0]["conversation_id"])

    second_events = await _collect_events(
        path=workbook_path,
        file_id="file-workbook-override",
        chat_text="Continue with the same analysis.",
        mode="text",
        sheet_index=1,
        sheet_override=True,
        locale="en",
        conversation_id=conversation_id,
    )

    assert second_events[0]["meta"]["sheet_override"] is True
    assert second_events[0]["meta"]["resolved_sheet_index"] == 1
    assert second_events[1]["pipeline"]["sheet_routing"]["reason"] == "manual_sheet_override"


def test_conversation_store_marks_explanation_question_as_followup() -> None:
    store = ConversationStore(max_sessions=4, max_turns=8)
    session, _ = store.ensure_session(
        conversation_id="conv-explicit-explain",
        file_id="file-1",
        sheet_index=1,
        locale="zh-CN",
    )
    store.append_turn(
        session,
        {
            "question": "不要返回表格的5行，应该是返回5个消费最高项和对应的费用",
            "requested_mode": "auto",
            "mode": "text",
            "intent": "ranking",
            "pipeline_summary": {
                "intent": "ranking",
                "mode": "text",
                "groupby": ["产品信息/计费项名称"],
                "metric_aliases": ["value"],
                "result_columns": ["产品信息/计费项名称", "value"],
                "result_row_count": 5,
            },
            "selection_plan": {
                "columns": ["产品信息/计费项名称", "应付信息/应付金额（含税）"],
                "filters": [],
                "distinct_by": None,
                "sort": None,
                "limit": None,
            },
            "transform_plan": {
                "derived_columns": [],
                "groupby": ["产品信息/计费项名称"],
                "metrics": [{"agg": "sum", "col": "应付信息/应付金额（含税）", "as_name": "value"}],
                "formula_metrics": [],
                "having": [],
                "pivot": None,
                "post_pivot_formula_metrics": [],
                "post_pivot_having": [],
                "return_rows": False,
                "order_by": {"col": "value", "dir": "desc"},
                "top_k": 5,
            },
            "result_columns": ["产品信息/计费项名称", "value"],
            "result_row_count": 5,
            "answer_summary": "实例 目前排在第 1，数值为 1,940.59。",
            "execution_disclosure": {"data_scope": "exact_full_table", "exact_used": True},
        },
    )

    context = store.build_followup_context(session, chat_text="为什么实例消费这么高呢，能分析一下吗")

    assert isinstance(context, dict)
    assert context["is_followup"] is True
    assert context["last_turn"]["intent"] == "ranking"


def test_conversation_store_expires_cached_dataframe_after_ttl() -> None:
    store = ConversationStore(
        max_sessions=2,
        max_turns=4,
        cache_ttl_seconds=1,
        cache_max_entries=4,
    )
    session, _ = store.ensure_session(
        conversation_id="conv-cache-ttl",
        file_id="file-1",
        sheet_index=1,
        locale="en",
    )

    store.set_cached_dataframe(
        session,
        cache_key="analysis_sampled",
        cache_token="token-1",
        dataframe={"rows": 3},
        sheet_name="Sheet1",
    )
    assert store.get_cached_dataframe(session, cache_key="analysis_sampled", cache_token="token-1") == (
        {"rows": 3},
        "Sheet1",
    )

    time.sleep(1.1)

    assert store.get_cached_dataframe(session, cache_key="analysis_sampled", cache_token="token-1") is None


def test_conversation_store_evicts_oldest_cached_dataframe_when_limit_exceeded() -> None:
    store = ConversationStore(
        max_sessions=2,
        max_turns=4,
        cache_ttl_seconds=60,
        cache_max_entries=2,
    )
    session, _ = store.ensure_session(
        conversation_id="conv-cache-evict",
        file_id="file-1",
        sheet_index=1,
        locale="en",
    )

    store.set_cached_dataframe(
        session,
        cache_key="cache-a",
        cache_token="token-a",
        dataframe={"id": "a"},
        sheet_name="Sheet1",
    )
    store.set_cached_dataframe(
        session,
        cache_key="cache-b",
        cache_token="token-b",
        dataframe={"id": "b"},
        sheet_name="Sheet1",
    )
    store.set_cached_dataframe(
        session,
        cache_key="cache-c",
        cache_token="token-c",
        dataframe={"id": "c"},
        sheet_name="Sheet1",
    )

    assert store.get_cached_dataframe(session, cache_key="cache-a", cache_token="token-a") is None
    assert store.get_cached_dataframe(session, cache_key="cache-b", cache_token="token-b") == (
        {"id": "b"},
        "Sheet1",
    )
    assert store.get_cached_dataframe(session, cache_key="cache-c", cache_token="token-c") == (
        {"id": "c"},
        "Sheet1",
    )
