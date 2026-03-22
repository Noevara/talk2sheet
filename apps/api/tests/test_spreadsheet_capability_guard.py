from __future__ import annotations

from app.services.spreadsheet.quality.capability_guard import detect_unsupported_request


def test_detect_unsupported_request_marks_join_out_of_scope_when_key_or_shape_is_invalid() -> None:
    blocked = detect_unsupported_request(
        "Join sheet 1 and sheet 2 then list raw detail rows.",
        locale="en",
    )

    assert blocked is not None
    assert "cross_sheet" in blocked["reason_codes"]
    assert "join_beta_out_of_scope" in blocked["reason_codes"]
    assert "cross-sheet analysis is not supported yet" in blocked["message"].lower()
    assert "join key is missing" in blocked["message"].lower()
    assert isinstance(blocked.get("join_guard"), dict)
    assert blocked["join_guard"]["eligible"] is False
    assert "join_key_missing" in blocked["join_guard"]["reasons"]
    assert "join_non_aggregate_query" in blocked["join_guard"]["reasons"]


def test_detect_unsupported_request_marks_join_beta_candidate_as_controlled_gate() -> None:
    blocked = detect_unsupported_request(
        "Join Sales and Users by email and show top 5 regions by total amount.",
        locale="en",
    )

    assert blocked is not None
    assert "cross_sheet" in blocked["reason_codes"]
    assert "join_beta_candidate" in blocked["reason_codes"]
    assert "join beta" in blocked["message"].lower()
    assert isinstance(blocked.get("join_guard"), dict)
    assert blocked["join_guard"]["eligible"] is True
    assert blocked["join_guard"]["join_key"].lower() == "email"


def test_detect_unsupported_request_keeps_advanced_statistics_blocking() -> None:
    blocked = detect_unsupported_request(
        "Run a regression analysis on this workbook.",
        locale="en",
    )

    assert blocked is not None
    assert blocked["reason_codes"] == ["advanced_statistics"]
    assert "advanced statistics" in blocked["message"].lower()
