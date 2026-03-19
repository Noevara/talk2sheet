from __future__ import annotations

import re
from typing import Any

from ..contracts.workbook_models import WorkbookContext, WorkbookSheetProfile
from ..core.schema import Clarification
from ..pipeline.column_profile import normalize_text
from .router_types import SheetRoutingDecision


_SHEET_INDEX_PATTERNS = (
    re.compile(r"\bsheet\s*([0-9]+)\b", re.I),
    re.compile(r"工作表\s*([0-9]+)", re.I),
)


def _localized_text(locale: str, *, zh: str, en: str) -> str:
    return zh if str(locale or "").startswith("zh") else en


def _sheet_by_index(workbook: WorkbookContext, sheet_index: int | None) -> WorkbookSheetProfile | None:
    if sheet_index is None:
        return None
    return next((sheet for sheet in workbook.sheets if int(sheet.sheet_index) == int(sheet_index)), None)


def _sheet_by_name(workbook: WorkbookContext, sheet_name: str) -> WorkbookSheetProfile | None:
    target = normalize_text(sheet_name)
    if not target:
        return None
    return next((sheet for sheet in workbook.sheets if normalize_text(sheet.sheet_name) == target), None)


def _clarification_resolution_sheet(
    workbook: WorkbookContext,
    followup_context: dict[str, Any] | None,
) -> WorkbookSheetProfile | None:
    if not isinstance(followup_context, dict):
        return None
    resolution = followup_context.get("clarification_resolution")
    if not isinstance(resolution, dict) or str(resolution.get("kind") or "") != "sheet_resolution":
        return None
    selected_value = str(resolution.get("selected_value") or resolution.get("value") or "").strip()
    if not selected_value:
        return None
    if selected_value.isdigit():
        return _sheet_by_index(workbook, int(selected_value))
    return _sheet_by_name(workbook, selected_value)


def _explicit_sheet_reference(
    workbook: WorkbookContext,
    chat_text: str,
) -> WorkbookSheetProfile | None:
    text = str(chat_text or "")
    normalized_question = normalize_text(text)

    for pattern in _SHEET_INDEX_PATTERNS:
        match = pattern.search(text)
        if match is None:
            continue
        return _sheet_by_index(workbook, int(match.group(1)))

    matched_sheets = [sheet for sheet in workbook.sheets if normalize_text(sheet.sheet_name) and normalize_text(sheet.sheet_name) in normalized_question]
    if not matched_sheets:
        return None
    matched_sheets.sort(key=lambda sheet: len(sheet.sheet_name), reverse=True)
    return matched_sheets[0]


def _sheet_score(
    sheet: WorkbookSheetProfile,
    *,
    chat_text: str,
    requested_sheet_index: int,
    followup_context: dict[str, Any] | None,
) -> int:
    normalized_question = normalize_text(chat_text)
    score = 0
    if int(sheet.sheet_index) == int(requested_sheet_index):
        score += 8

    if isinstance(followup_context, dict) and bool(followup_context.get("is_followup")):
        last_sheet_index = int(followup_context.get("last_sheet_index") or 0)
        if last_sheet_index and int(sheet.sheet_index) == last_sheet_index:
            score += 12

    normalized_sheet_name = normalize_text(sheet.sheet_name)
    if normalized_sheet_name and normalized_sheet_name in normalized_question:
        score += 40

    for column in sheet.columns:
        normalized_column = normalize_text(column)
        if not normalized_column:
            continue
        if normalized_column in normalized_question:
            score += 18
        elif any(token and token in normalized_question for token in re.split(r"[^0-9a-zA-Z\u4e00-\u9fff]+", str(column)) if token):
            score += 4

    for profile in sheet.column_profile_summary:
        for hint in [str(item) for item in (profile.get("semantic_hints") or [])]:
            if hint and hint in normalized_question:
                score += 2

    return score


def _build_sheet_clarification(
    workbook: WorkbookContext,
    *,
    locale: str,
    candidate_scores: list[dict[str, Any]],
) -> Clarification:
    return Clarification(
        reason=_localized_text(
            locale,
            zh="这个问题可能对应多个工作表，请先确认要分析哪个 sheet。",
            en="This question may belong to multiple sheets. Choose the sheet first.",
        ),
        field=_localized_text(locale, zh="sheet", en="sheet"),
        options=[
            {
                "label": f"{item['sheet_name']} (Sheet {item['sheet_index']})",
                "value": item["sheet_name"],
                "sheet_index": item["sheet_index"],
                "score": item["score"],
            }
            for item in candidate_scores[: min(4, len(workbook.sheets))]
        ],
    )


def route_sheet(
    workbook: WorkbookContext,
    *,
    chat_text: str,
    requested_sheet_index: int,
    requested_sheet_override: bool = False,
    followup_context: dict[str, Any] | None,
    locale: str,
) -> SheetRoutingDecision:
    requested_sheet = _sheet_by_index(workbook, requested_sheet_index)
    if len(workbook.sheets) <= 1:
        single_sheet = workbook.sheets[0] if workbook.sheets else None
        return SheetRoutingDecision(
            status="resolved",
            requested_sheet_index=requested_sheet_index,
            resolved_sheet_index=single_sheet.sheet_index if single_sheet is not None else requested_sheet_index,
            resolved_sheet_name=single_sheet.sheet_name if single_sheet is not None else "",
            reason="single_sheet_workbook",
            matched_by="single_sheet",
            confidence=1.0,
        )

    clarified_sheet = _clarification_resolution_sheet(workbook, followup_context)
    if clarified_sheet is not None:
        return SheetRoutingDecision(
            status="resolved",
            requested_sheet_index=requested_sheet_index,
            resolved_sheet_index=clarified_sheet.sheet_index,
            resolved_sheet_name=clarified_sheet.sheet_name,
            reason="clarification_resolution",
            matched_by="clarification_resolution",
            confidence=1.0,
        )

    explicit_sheet = _explicit_sheet_reference(workbook, chat_text)
    if explicit_sheet is not None:
        return SheetRoutingDecision(
            status="resolved",
            requested_sheet_index=requested_sheet_index,
            resolved_sheet_index=explicit_sheet.sheet_index,
            resolved_sheet_name=explicit_sheet.sheet_name,
            reason="explicit_sheet_reference",
            matched_by="explicit_reference",
            confidence=1.0,
        )

    if requested_sheet_override and requested_sheet is not None:
        return SheetRoutingDecision(
            status="resolved",
            requested_sheet_index=requested_sheet_index,
            resolved_sheet_index=requested_sheet.sheet_index,
            resolved_sheet_name=requested_sheet.sheet_name,
            reason="manual_sheet_override",
            matched_by="requested_override",
            confidence=0.98,
        )

    if isinstance(followup_context, dict) and bool(followup_context.get("is_followup")):
        last_sheet_index = int(followup_context.get("last_sheet_index") or 0)
        inherited_sheet = _sheet_by_index(workbook, last_sheet_index)
        if inherited_sheet is not None:
            return SheetRoutingDecision(
                status="resolved",
                requested_sheet_index=requested_sheet_index,
                resolved_sheet_index=inherited_sheet.sheet_index,
                resolved_sheet_name=inherited_sheet.sheet_name,
                reason="followup_inherit_previous_sheet",
                matched_by="followup",
                confidence=0.92,
            )

    candidate_scores = [
        {
            "sheet_index": sheet.sheet_index,
            "sheet_name": sheet.sheet_name,
            "score": _sheet_score(
                sheet,
                chat_text=chat_text,
                requested_sheet_index=requested_sheet_index,
                followup_context=followup_context,
            ),
        }
        for sheet in workbook.sheets
    ]
    candidate_scores.sort(key=lambda item: (int(item["score"]), -int(item["sheet_index"])), reverse=True)
    best = candidate_scores[0] if candidate_scores else None
    second = candidate_scores[1] if len(candidate_scores) > 1 else None

    if best is not None and int(best["score"]) >= 18 and (second is None or int(best["score"]) - int(second["score"]) >= 8):
        return SheetRoutingDecision(
            status="resolved",
            requested_sheet_index=requested_sheet_index,
            resolved_sheet_index=int(best["sheet_index"]),
            resolved_sheet_name=str(best["sheet_name"]),
            reason="auto_routed_by_sheet_profile",
            matched_by="auto_routing",
            confidence=min(0.95, 0.55 + (int(best["score"]) / 100.0)),
            candidate_scores=candidate_scores,
        )

    if best is not None and second is not None and int(best["score"]) >= 18 and abs(int(best["score"]) - int(second["score"])) <= 6:
        return SheetRoutingDecision(
            status="clarification",
            requested_sheet_index=requested_sheet_index,
            resolved_sheet_index=None,
            resolved_sheet_name="",
            reason="ambiguous_sheet_match",
            matched_by="auto_routing",
            confidence=0.0,
            clarification=_build_sheet_clarification(workbook, locale=locale, candidate_scores=candidate_scores),
            candidate_scores=candidate_scores,
        )

    fallback_sheet = requested_sheet or (workbook.sheets[0] if workbook.sheets else None)
    return SheetRoutingDecision(
        status="resolved",
        requested_sheet_index=requested_sheet_index,
        resolved_sheet_index=fallback_sheet.sheet_index if fallback_sheet is not None else requested_sheet_index,
        resolved_sheet_name=fallback_sheet.sheet_name if fallback_sheet is not None else "",
        reason="requested_sheet_default",
        matched_by="requested_sheet",
        confidence=0.5,
        candidate_scores=candidate_scores,
    )
