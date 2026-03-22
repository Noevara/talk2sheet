from __future__ import annotations

from pathlib import Path

import pandas as pd

from app.services.spreadsheet.contracts.workbook_models import WorkbookContext, WorkbookSheetProfile
from app.services.spreadsheet.execution.join_executor import execute_join_beta
from app.schemas import JoinPreflightResult, JoinPreflightSheetMetrics


def _write_joinable_workbook(path: Path) -> None:
    with pd.ExcelWriter(path) as writer:
        pd.DataFrame(
            {
                "Email": ["a@test.com", "b@test.com", "a@test.com", "d@test.com"],
                "Amount": [100, 80, 30, 20],
                "OrderDate": ["2025-01-01", "2025-01-02", "2025-01-04", "2025-01-07"],
            }
        ).to_excel(writer, sheet_name="Orders", index=False)
        pd.DataFrame(
            {
                "Email": ["a@test.com", "b@test.com", "c@test.com"],
                "Region": ["SH", "BJ", "GZ"],
            }
        ).to_excel(writer, sheet_name="Users", index=False)


def _workbook_context() -> WorkbookContext:
    return WorkbookContext(
        file_id="join-beta-file",
        active_sheet_index=1,
        active_sheet_name="Orders",
        sheets=[
            WorkbookSheetProfile(sheet_index=1, sheet_name="Orders", columns=["Email", "Amount", "OrderDate"]),
            WorkbookSheetProfile(sheet_index=2, sheet_name="Users", columns=["Email", "Region"]),
        ],
    )


def _preflight(status: str = "pass") -> JoinPreflightResult:
    return JoinPreflightResult(
        status=status,  # type: ignore[arg-type]
        is_join_request=True,
        join_key="email",
        join_type="inner",
        sheet_indexes=[1, 2],
        left_sheet=JoinPreflightSheetMetrics(sheet_index=1, sheet_name="Orders", key_column="Email"),
        right_sheet=JoinPreflightSheetMetrics(sheet_index=2, sheet_name="Users", key_column="Email"),
        checks=[],
        repair_suggestions=[],
        summary="ok",
    )


def test_execute_join_beta_returns_topn_result_with_join_quality(tmp_path) -> None:
    workbook_path = tmp_path / "join_beta_topn.xlsx"
    _write_joinable_workbook(workbook_path)

    payload = execute_join_beta(
        path=workbook_path,
        workbook_context=_workbook_context(),
        preflight=_preflight("pass"),
        question="Join Orders and Users by email and show top 2 regions by total amount.",
        requested_mode="auto",
        locale="en",
    )

    assert payload.mode == "text"
    assert payload.pipeline["status"] == "ok"
    assert payload.pipeline["planner"]["provider"] == "join_beta_executor"
    assert payload.pipeline["join_beta"]["executed"] is True
    assert payload.pipeline["join_beta"]["join_key"] == "email"
    assert payload.pipeline["join_beta"]["join_type"] == "inner"
    assert payload.pipeline["join_beta"]["matched_rows"] >= 1
    assert payload.pipeline["result_row_count"] >= 1
    assert len(payload.pipeline["preview_rows"]) >= 1
    assert "join beta" in payload.answer.lower()


def test_execute_join_beta_handles_conversion_question(tmp_path) -> None:
    workbook_path = tmp_path / "join_beta_conversion.xlsx"
    _write_joinable_workbook(workbook_path)

    payload = execute_join_beta(
        path=workbook_path,
        workbook_context=_workbook_context(),
        preflight=_preflight("warn"),
        question="Join Orders and Users by email and show conversion rate.",
        requested_mode="text",
        locale="en",
    )

    assert payload.pipeline["join_beta"]["intent"] == "conversion"
    assert payload.pipeline["result_row_count"] == 1
    first_row = payload.pipeline["preview_rows"][0]
    assert any(str(cell) == "conversion_rate" for cell in payload.pipeline["result_columns"]) or len(first_row) >= 3
