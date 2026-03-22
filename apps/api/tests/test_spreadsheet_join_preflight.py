from __future__ import annotations

from pathlib import Path

import pandas as pd

from app.services.spreadsheet.pipeline import read_workbook_context
from app.services.spreadsheet.pipeline.join_preflight import run_join_preflight
from app.services.spreadsheet.routing.sheet_router import route_sheet


def _write_missing_key_workbook(path: Path) -> None:
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
            }
        ).to_excel(writer, sheet_name="Users", index=False)


def _write_valid_key_workbook(path: Path) -> None:
    with pd.ExcelWriter(path) as writer:
        pd.DataFrame(
            {
                "Email": ["a@test.com", "b@test.com", "a@test.com"],
                "Amount": [120, 80, 50],
                "OrderDate": ["2025-01-01", "2025-01-02", "2025-01-03"],
            }
        ).to_excel(writer, sheet_name="Orders", index=False)
        pd.DataFrame(
            {
                "Email": ["a@test.com", "b@test.com", "c@test.com"],
                "Region": ["SH", "BJ", "GZ"],
            }
        ).to_excel(writer, sheet_name="Users", index=False)


def test_run_join_preflight_returns_fail_with_repair_suggestions_for_missing_key(tmp_path) -> None:
    workbook_path = tmp_path / "join_preflight_missing_key.xlsx"
    _write_missing_key_workbook(workbook_path)

    workbook_context = read_workbook_context(
        workbook_path,
        file_id="join-preflight-missing",
        active_sheet_index=1,
        preview_limit=8,
    )
    routing_decision = route_sheet(
        workbook_context,
        chat_text="Join Sales and Users by email and show conversion.",
        requested_sheet_index=1,
        requested_sheet_override=False,
        followup_context=None,
        locale="en",
    )

    result = run_join_preflight(
        path=workbook_path,
        workbook_context=workbook_context,
        routing_decision=routing_decision,
        question="Join Sales and Users by email and show conversion.",
        locale="en",
    )

    assert result is not None
    assert result.status == "fail"
    assert result.is_join_request is True
    assert any(check.code == "left_sheet_key_missing" for check in result.checks)
    assert len(result.repair_suggestions) >= 1
    assert "preflight failed" in result.summary.lower()


def test_run_join_preflight_returns_pass_for_single_key_aggregate_join_candidate(tmp_path) -> None:
    workbook_path = tmp_path / "join_preflight_pass.xlsx"
    _write_valid_key_workbook(workbook_path)

    workbook_context = read_workbook_context(
        workbook_path,
        file_id="join-preflight-pass",
        active_sheet_index=1,
        preview_limit=8,
    )
    question = "Join Orders and Users by email and show top 3 regions by total amount."
    routing_decision = route_sheet(
        workbook_context,
        chat_text=question,
        requested_sheet_index=1,
        requested_sheet_override=False,
        followup_context=None,
        locale="en",
    )

    result = run_join_preflight(
        path=workbook_path,
        workbook_context=workbook_context,
        routing_decision=routing_decision,
        question=question,
        locale="en",
    )

    assert result is not None
    assert result.status == "pass"
    assert result.is_join_request is True
    assert result.join_key.lower() == "email"
    assert result.estimated_match_rate is not None
    assert result.estimated_match_rate >= 0.4
    assert result.left_sheet is not None
    assert result.right_sheet is not None
