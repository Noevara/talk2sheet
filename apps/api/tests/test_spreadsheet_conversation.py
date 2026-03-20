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
    assert len(second_context["recent_pipeline_history"]) == 1
    assert second_context["recent_pipeline_history"][0]["pipeline_summary"]["intent"] == "row_count"
    assert second_context["recent_pipeline_history"][0]["pipeline_summary"]["analysis_intent"]["kind"] == "row_count"
    assert second_context["recent_pipeline_history"][0]["execution_disclosure"]["data_scope"] == "exact_full_table"

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
    assert events[1]["pipeline"]["source_sheet_index"] == 2
    assert events[1]["pipeline"]["source_sheet_name"] == "Users"
    assert events[1]["pipeline"]["sheet_routing"]["reason"] == "auto_routed_by_sheet_profile"


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
    assert second_events[0]["meta"]["resolved_sheet_index"] == 2
    assert second_events[1]["pipeline"]["source_sheet_index"] == 2


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
