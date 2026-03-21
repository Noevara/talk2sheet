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


def _three_sheet_workbook_context() -> WorkbookContext:
    return WorkbookContext(
        file_id="file-3",
        active_sheet_index=1,
        active_sheet_name="Sales",
        sheets=[
            WorkbookSheetProfile(sheet_index=1, sheet_name="Sales", columns=["Date", "Amount"], column_profile_summary=[]),
            WorkbookSheetProfile(sheet_index=2, sheet_name="Users", columns=["User Name", "Email"], column_profile_summary=[]),
            WorkbookSheetProfile(sheet_index=3, sheet_name="Costs", columns=["Date", "Cost"], column_profile_summary=[]),
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


def test_sheet_router_switches_to_other_sheet_for_followup_request_in_two_sheet_workbook() -> None:
    decision = route_sheet(
        _workbook_context(),
        chat_text="Please continue on another sheet.",
        requested_sheet_index=1,
        requested_sheet_override=False,
        followup_context={
            "is_followup": True,
            "last_sheet_index": 1,
            "last_sheet_name": "Sales",
            "wants_sheet_switch": True,
            "visited_sheets": [
                {"sheet_index": 1, "sheet_name": "Sales"},
            ],
        },
        locale="en",
    )

    assert decision.status == "resolved"
    assert decision.resolved_sheet_index == 2
    assert decision.reason == "followup_switch_to_another_sheet"


def test_sheet_router_returns_switch_clarification_when_followup_switch_is_ambiguous() -> None:
    decision = route_sheet(
        _three_sheet_workbook_context(),
        chat_text="再看另一个 sheet。",
        requested_sheet_index=1,
        requested_sheet_override=False,
        followup_context={
            "is_followup": True,
            "last_sheet_index": 1,
            "last_sheet_name": "Sales",
            "wants_sheet_switch": True,
            "visited_sheets": [
                {"sheet_index": 1, "sheet_name": "Sales"},
            ],
        },
        locale="zh-CN",
    )

    assert decision.status == "clarification"
    assert decision.reason == "followup_sheet_switch_clarification"
    assert decision.clarification is not None
    assert decision.clarification.field == "sheet"
    assert len(decision.clarification.options) == 2
    option_values = [item["value"] for item in decision.clarification.options]
    assert "Users" in option_values
    assert "Costs" in option_values


def test_sheet_router_can_switch_back_to_previous_sheet_for_followup_request() -> None:
    decision = route_sheet(
        _workbook_context(),
        chat_text="回到上一个 sheet 继续。",
        requested_sheet_index=2,
        requested_sheet_override=False,
        followup_context={
            "is_followup": True,
            "last_sheet_index": 2,
            "last_sheet_name": "Users",
            "wants_sheet_switch": True,
            "wants_previous_sheet": True,
            "sheet_reference_hint": "previous",
            "previous_sheet_index": 1,
            "previous_sheet_name": "Sales",
            "recent_sheet_trajectory": [
                {"sheet_index": 1, "sheet_name": "Sales"},
                {"sheet_index": 2, "sheet_name": "Users"},
            ],
        },
        locale="zh-CN",
    )

    assert decision.status == "resolved"
    assert decision.resolved_sheet_index == 1
    assert decision.resolved_sheet_name == "Sales"
    assert decision.reason == "followup_switch_to_previous_sheet"


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


def test_sheet_router_marks_boundary_for_multi_sheet_question() -> None:
    decision = route_sheet(
        _workbook_context(),
        chat_text="Compare Sales and Users, then summarize each sheet.",
        requested_sheet_index=1,
        requested_sheet_override=False,
        followup_context=None,
        locale="en",
    )

    assert decision.status == "clarification"
    assert decision.reason == "multi_sheet_needs_decomposition"
    assert decision.clarification is not None
    assert decision.clarification.field == "sheet"
    assert len(decision.clarification.options) == 2
    assert decision.boundary_status == "multi_sheet_detected"
    assert decision.boundary_reason == "multi_sheet_query_detected"
    assert "Suggested decomposition" in decision.decomposition_hint
    assert [item["sheet_index"] for item in decision.mentioned_sheets] == [1, 2]


def test_sheet_router_marks_boundary_out_of_scope_for_cross_sheet_join() -> None:
    decision = route_sheet(
        _workbook_context(),
        chat_text="Join Sales and Users by email and calculate conversion.",
        requested_sheet_index=1,
        requested_sheet_override=False,
        followup_context=None,
        locale="en",
    )

    assert decision.status == "clarification"
    assert decision.reason == "multi_sheet_needs_decomposition"
    assert decision.clarification is not None
    assert len(decision.clarification.options) == 2
    assert decision.boundary_status == "multi_sheet_out_of_scope"
    assert decision.boundary_reason == "cross_sheet_join_not_supported"
    assert "join is not supported yet" in decision.clarification.reason
    assert [item["sheet_name"] for item in decision.mentioned_sheets] == ["Sales", "Users"]


def test_sheet_router_multi_sheet_clarification_reason_localized_for_zh() -> None:
    decision = route_sheet(
        _workbook_context(),
        chat_text="把 Sales 和 Users 合并分析一下。",
        requested_sheet_index=1,
        requested_sheet_override=False,
        followup_context=None,
        locale="zh-CN",
    )

    assert decision.status == "clarification"
    assert decision.clarification is not None
    assert "当前不支持跨 sheet 联合分析" in decision.clarification.reason
    assert "建议拆解" in decision.decomposition_hint
