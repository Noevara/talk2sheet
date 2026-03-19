from __future__ import annotations

from app.services.spreadsheet.contracts.workbook_models import WorkbookContext, WorkbookSheetProfile
from app.services.spreadsheet.routing.sheet_router import route_sheet


def _workbook_context() -> WorkbookContext:
    return WorkbookContext(
        file_id="file-1",
        active_sheet_index=1,
        active_sheet_name="Sales",
        sheets=[
            WorkbookSheetProfile(
                sheet_index=1,
                sheet_name="Sales",
                columns=["Date", "Amount", "Region"],
                total_rows=3,
                preview_row_count=3,
                column_profile_summary=[
                    {"name": "Date", "semantic_type": "date", "semantic_hints": ["date"]},
                    {"name": "Amount", "semantic_type": "numeric", "semantic_hints": ["amount"]},
                    {"name": "Region", "semantic_type": "categorical", "semantic_hints": ["category"]},
                ],
            ),
            WorkbookSheetProfile(
                sheet_index=2,
                sheet_name="Users",
                columns=["User Name", "Email", "Signup Date"],
                total_rows=2,
                preview_row_count=2,
                column_profile_summary=[
                    {"name": "User Name", "semantic_type": "text", "semantic_hints": ["name"]},
                    {"name": "Email", "semantic_type": "id", "semantic_hints": ["id"]},
                    {"name": "Signup Date", "semantic_type": "date", "semantic_hints": ["date"]},
                ],
            ),
        ],
    )


def test_sheet_router_resolves_explicit_sheet_name_reference() -> None:
    decision = route_sheet(
        _workbook_context(),
        chat_text="Show me the total amount in Sales.",
        requested_sheet_index=1,
        requested_sheet_override=False,
        followup_context=None,
        locale="en",
    )

    assert decision.status == "resolved"
    assert decision.resolved_sheet_index == 1
    assert decision.matched_by == "explicit_reference"


def test_sheet_router_resolves_followup_to_previous_sheet() -> None:
    decision = route_sheet(
        _workbook_context(),
        chat_text="Continue with the same analysis.",
        requested_sheet_index=1,
        requested_sheet_override=False,
        followup_context={
            "is_followup": True,
            "last_sheet_index": 2,
            "last_sheet_name": "Users",
        },
        locale="en",
    )

    assert decision.status == "resolved"
    assert decision.resolved_sheet_index == 2
    assert decision.reason == "followup_inherit_previous_sheet"


def test_sheet_router_can_auto_route_by_column_match() -> None:
    decision = route_sheet(
        _workbook_context(),
        chat_text="How many email records are there?",
        requested_sheet_index=1,
        requested_sheet_override=False,
        followup_context=None,
        locale="en",
    )

    assert decision.status == "resolved"
    assert decision.resolved_sheet_index == 2
    assert decision.matched_by == "auto_routing"


def test_sheet_router_can_return_clarification_for_ambiguous_column_match() -> None:
    workbook = WorkbookContext(
        file_id="file-2",
        active_sheet_index=1,
        active_sheet_name="Users",
        sheets=[
            WorkbookSheetProfile(sheet_index=1, sheet_name="Users", columns=["Name", "Email"], column_profile_summary=[{"name": "Name", "semantic_type": "text", "semantic_hints": ["name"]}]),
            WorkbookSheetProfile(sheet_index=2, sheet_name="Products", columns=["Name", "Price"], column_profile_summary=[{"name": "Name", "semantic_type": "text", "semantic_hints": ["name"]}]),
        ],
    )

    decision = route_sheet(
        workbook,
        chat_text="Show me the names.",
        requested_sheet_index=0,
        requested_sheet_override=False,
        followup_context=None,
        locale="en",
    )

    assert decision.status == "clarification"
    assert decision.clarification is not None
    assert decision.clarification.field == "sheet"
    assert len(decision.clarification.options) == 2


def test_sheet_router_can_use_sheet_clarification_resolution() -> None:
    decision = route_sheet(
        _workbook_context(),
        chat_text="Use the selected sheet.",
        requested_sheet_index=1,
        requested_sheet_override=False,
        followup_context={
            "is_followup": True,
            "clarification_resolution": {
                "kind": "sheet_resolution",
                "selected_value": "Users",
            },
        },
        locale="en",
    )

    assert decision.status == "resolved"
    assert decision.resolved_sheet_index == 2
    assert decision.reason == "clarification_resolution"


def test_sheet_router_prefers_manual_sheet_override_over_followup_inheritance() -> None:
    decision = route_sheet(
        _workbook_context(),
        chat_text="Continue with the same analysis.",
        requested_sheet_index=1,
        requested_sheet_override=True,
        followup_context={
            "is_followup": True,
            "last_sheet_index": 2,
            "last_sheet_name": "Users",
        },
        locale="en",
    )

    assert decision.status == "resolved"
    assert decision.resolved_sheet_index == 1
    assert decision.reason == "manual_sheet_override"
