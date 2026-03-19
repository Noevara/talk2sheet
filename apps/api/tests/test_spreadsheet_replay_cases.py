from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from app.config import get_settings
from app.services.spreadsheet.analysis import analyze
from app.services.spreadsheet.conversation.conversation_memory import ConversationStore, build_turn_summary
from app.services.spreadsheet.core.schema import ChartSpec, SelectionPlan, TransformPlan
from app.services.spreadsheet.execution.exact_executor import execute_exact_plan
from app.services.spreadsheet.pipeline.column_profile import attach_column_profiles
from app.services.spreadsheet.quality.repair import repair_chart_spec, repair_selection_plan, repair_transform_plan
from app.services.spreadsheet.quality.validator import build_clarification, validate_chart_spec, validate_selection_plan


FIXTURE_ROOT = Path(__file__).with_name("fixtures") / "replay"
CASE_ROOT = FIXTURE_ROOT / "cases"
DATA_ROOT = FIXTURE_ROOT / "data"


@pytest.fixture(autouse=True)
def _stable_replay_settings(monkeypatch):
    monkeypatch.setenv("TALK2SHEET_PLANNER_PROVIDER", "heuristic")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _load_fixture_dataframe(local_file: str, *, sheet_index: int = 1):
    path = DATA_ROOT / local_file
    suffix = path.suffix.lower()
    if suffix == ".csv":
        df = pd.read_csv(path)
    elif suffix in {".xlsx", ".xls"}:
        df = pd.read_excel(path, sheet_name=max(sheet_index - 1, 0))
    else:
        raise AssertionError(f"Unsupported replay fixture file type: {path.name}")
    return attach_column_profiles(df)


def _load_cases() -> list[dict[str, object]]:
    out: list[dict[str, object]] = []
    for fixture_path in sorted(CASE_ROOT.glob("*.json")):
        payload = json.loads(fixture_path.read_text(encoding="utf-8"))
        if isinstance(payload, list):
            out.extend(payload)
        else:
            raise AssertionError(f"Replay fixture must be a list: {fixture_path}")
    return out


def _assert_analysis_case(result, case: dict[str, object]) -> None:
    if case.get("expected_mode") is not None:
        assert result.mode == case["expected_mode"]
    assert result.pipeline["planner"]["intent"] == case["expected_intent"]
    assert result.pipeline["result_columns"] == case["expected_result_columns"]
    assert str(case["expected_answer_contains"]) in result.answer
    if isinstance(case.get("expected_preview_rows"), list):
        assert result.pipeline.get("preview_rows") == case["expected_preview_rows"]
    if isinstance(case.get("expected_chart_spec"), dict):
        assert result.chart_spec is not None
        for key, value in case["expected_chart_spec"].items():
            assert result.chart_spec.get(key) == value


@pytest.mark.parametrize("case", _load_cases(), ids=lambda case: str(case["id"]))
def test_replay_cases(case: dict[str, object]) -> None:
    replay_type = str(case["type"])
    df = _load_fixture_dataframe(str(case["local_file"]), sheet_index=int(case.get("sheet_index") or 1))

    if replay_type == "analysis":
        result = analyze(
            df,
            chat_text=str(case["chat_text"]),
            requested_mode=str(case["requested_mode"]),
            locale=str(case["locale"]),
            rows_loaded=int(len(df.index)),
            followup_context=case.get("followup_context") if isinstance(case.get("followup_context"), dict) else None,
        )

        _assert_analysis_case(result, case)
        return

    if replay_type == "analysis_sequence":
        store = ConversationStore(max_sessions=4, max_turns=8)
        session, _ = store.ensure_session(
            conversation_id=str(case.get("conversation_id") or case["id"]),
            file_id=str(case["local_file"]),
            sheet_index=int(case.get("sheet_index") or 1),
            locale=str(case.get("locale") or "en"),
        )
        turns = case.get("turns")
        assert isinstance(turns, list) and turns
        for turn in turns:
            assert isinstance(turn, dict)
            result = analyze(
                df,
                chat_text=str(turn["chat_text"]),
                requested_mode=str(turn["requested_mode"]),
                locale=str(turn["locale"]),
                rows_loaded=int(len(df.index)),
                followup_context=store.build_followup_context(session, chat_text=str(turn["chat_text"])),
            )
            _assert_analysis_case(result, turn)
            store.append_turn(
                session,
                build_turn_summary(
                    question=str(turn["chat_text"]),
                    requested_mode=str(turn["requested_mode"]),
                    result_mode=result.mode,
                    pipeline=result.pipeline,
                    answer=result.answer,
                    analysis_text=result.analysis_text,
                    chart_spec=result.chart_spec,
                    execution_disclosure=result.execution_disclosure.model_dump(),
                ),
            )
        return

    if replay_type == "transform":
        selection_plan = SelectionPlan.model_validate(case["selection_plan"])
        transform_plan = TransformPlan.model_validate(case["transform_plan"])
        result_df, _meta = execute_exact_plan(df, selection_plan, transform_plan)
        expected_records = case["expected_records"]
        assert result_df.to_dict(orient="records") == expected_records
        return

    if replay_type == "selection_repair":
        selection_plan = SelectionPlan.model_validate(case["selection_plan"])
        repaired_plan, _meta = repair_selection_plan(df, selection_plan, question="repair replay", mode="text")
        expected = case["expected_selection_plan"]
        assert repaired_plan.columns == expected["columns"]
        assert repaired_plan.sort is not None
        assert repaired_plan.sort.col == expected["sort_col"]
        return

    if replay_type == "transform_repair":
        transform_plan = TransformPlan.model_validate(case["transform_plan"])
        repaired_plan, _meta = repair_transform_plan(df, transform_plan, question="transform repair replay", mode="text")
        expected = case["expected_transform_plan"]
        assert repaired_plan.groupby == expected["groupby"]
        assert [metric.col for metric in repaired_plan.metrics] == expected["metric_cols"]
        assert repaired_plan.pivot is not None
        assert repaired_plan.pivot.index == expected["pivot_index"]
        assert repaired_plan.pivot.columns == expected["pivot_columns"]
        assert repaired_plan.pivot.values == expected["pivot_values"]
        assert repaired_plan.order_by is not None
        assert repaired_plan.order_by.col == expected["order_by_col"]
        return

    if replay_type == "chart_repair":
        chart_spec = ChartSpec.model_validate(case["chart_spec"])
        repaired_spec, _meta = repair_chart_spec(df, chart_spec, question="chart repair replay")
        expected = case["expected_chart_spec"]
        assert repaired_spec.x == expected["x"]
        assert repaired_spec.y == expected["y"]
        assert repaired_spec.title == expected["title"]
        return

    if replay_type == "selection_clarification":
        selection_plan = SelectionPlan.model_validate(case["selection_plan"])
        issues = validate_selection_plan(df, selection_plan, question="clarification replay", mode="text")
        clarification = build_clarification(issues)
        assert clarification is not None
        assert str(case["expected_reason_contains"]) in clarification.reason
        labels = [str(option["label"]) for option in clarification.options]
        for expected in case["expected_option_labels_include"]:
            assert str(expected) in labels
        return

    if replay_type == "chart_clarification":
        chart_spec = ChartSpec.model_validate(case["chart_spec"])
        issues = validate_chart_spec(df, chart_spec)
        clarification = build_clarification(issues)
        assert clarification is not None
        assert str(case["expected_reason_contains"]) in clarification.reason
        labels = [str(option["label"]) for option in clarification.options]
        for expected in case["expected_option_labels_include"]:
            assert str(expected) in labels
        return

    raise AssertionError(f"Unsupported replay type: {replay_type}")
