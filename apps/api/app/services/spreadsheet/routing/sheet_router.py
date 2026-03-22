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

_MULTI_SHEET_REFERENCE_PATTERNS = (
    re.compile(r"\bmultiple\s+sheets?\b", re.I),
    re.compile(r"\banother\s+sheet\b", re.I),
    re.compile(r"\bother\s+sheet\b", re.I),
    re.compile(r"\bseveral\s+sheets?\b", re.I),
    re.compile(r"多个\s*sheet", re.I),
    re.compile(r"多个工作表", re.I),
    re.compile(r"另一[个張]\s*sheet", re.I),
    re.compile(r"另一个工作表", re.I),
    re.compile(r"別のシート", re.I),
    re.compile(r"複数のシート", re.I),
)

_FOLLOWUP_SHEET_SWITCH_PATTERNS = (
    re.compile(r"\banother\s+sheet\b", re.I),
    re.compile(r"\bother\s+sheet\b", re.I),
    re.compile(r"\bnext\s+sheet\b", re.I),
    re.compile(r"\bswitch\s+(to\s+)?(another|other)\s+sheet\b", re.I),
    re.compile(r"另一个\s*sheet", re.I),
    re.compile(r"另外一个\s*sheet", re.I),
    re.compile(r"另一个工作表", re.I),
    re.compile(r"另外一个工作表", re.I),
    re.compile(r"换(到|成)?另一个\s*sheet", re.I),
    re.compile(r"再看另一个", re.I),
    re.compile(r"別のシート", re.I),
    re.compile(r"他のシート", re.I),
    re.compile(r"別シート", re.I),
)

_FOLLOWUP_PREVIOUS_SHEET_PATTERNS = (
    re.compile(r"\bprevious\s+sheet\b", re.I),
    re.compile(r"\blast\s+sheet\b", re.I),
    re.compile(r"\bback\s+to\s+(the\s+)?previous\s+sheet\b", re.I),
    re.compile(r"上一个\s*sheet", re.I),
    re.compile(r"上一张\s*sheet", re.I),
    re.compile(r"上一个工作表", re.I),
    re.compile(r"上一张工作表", re.I),
    re.compile(r"回到上一个\s*sheet", re.I),
    re.compile(r"回到上一个工作表", re.I),
    re.compile(r"前一个\s*sheet", re.I),
    re.compile(r"前一个工作表", re.I),
    re.compile(r"前のシート", re.I),
)

_FOLLOWUP_CURRENT_SHEET_PATTERNS = (
    re.compile(r"\bcurrent\s+sheet\b", re.I),
    re.compile(r"\bthis\s+sheet\b", re.I),
    re.compile(r"\bkeep\s+current\s+sheet\b", re.I),
    re.compile(r"当前\s*sheet", re.I),
    re.compile(r"这个\s*sheet", re.I),
    re.compile(r"当前工作表", re.I),
    re.compile(r"这个工作表", re.I),
    re.compile(r"本\s*sheet", re.I),
    re.compile(r"本工作表", re.I),
    re.compile(r"今のシート", re.I),
    re.compile(r"現在のシート", re.I),
)

_CROSS_SHEET_ANALYSIS_PATTERNS = (
    re.compile(r"\bjoin\b", re.I),
    re.compile(r"\bmerge\b", re.I),
    re.compile(r"\bunion\b", re.I),
    re.compile(r"\bcombine\b", re.I),
    re.compile(r"跨\s*sheet", re.I),
    re.compile(r"跨\s*表", re.I),
    re.compile(r"关联分析", re.I),
    re.compile(r"联合分析", re.I),
    re.compile(r"合并分析", re.I),
    re.compile(r"シート.*結合", re.I),
)


def _localized_text(locale: str, *, zh: str, en: str, ja: str | None = None) -> str:
    normalized = str(locale or "").lower()
    if normalized.startswith("zh"):
        return zh
    if normalized.startswith("ja"):
        return ja or en
    return en


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


def _mentioned_sheets(workbook: WorkbookContext, chat_text: str) -> list[WorkbookSheetProfile]:
    text = str(chat_text or "")
    normalized_question = normalize_text(text)
    mentioned: dict[int, WorkbookSheetProfile] = {}

    for pattern in _SHEET_INDEX_PATTERNS:
        for match in pattern.finditer(text):
            sheet = _sheet_by_index(workbook, int(match.group(1)))
            if sheet is not None:
                mentioned[int(sheet.sheet_index)] = sheet

    for sheet in workbook.sheets:
        normalized_sheet_name = normalize_text(sheet.sheet_name)
        if normalized_sheet_name and normalized_sheet_name in normalized_question:
            mentioned[int(sheet.sheet_index)] = sheet

    return [mentioned[index] for index in sorted(mentioned)]


def _has_pattern_match(patterns: tuple[re.Pattern[str], ...], chat_text: str) -> bool:
    text = str(chat_text or "")
    return any(pattern.search(text) is not None for pattern in patterns)


def _boundary_metadata(
    workbook: WorkbookContext,
    *,
    chat_text: str,
) -> tuple[str, str, list[dict[str, Any]]]:
    if len(workbook.sheets) <= 1:
        return "single_sheet_in_scope", "single_sheet_query", []

    mentioned = _mentioned_sheets(workbook, chat_text)
    mentioned_payload = [
        {
            "sheet_index": int(sheet.sheet_index),
            "sheet_name": str(sheet.sheet_name),
        }
        for sheet in mentioned
    ]

    multi_sheet_signal = _has_pattern_match(_MULTI_SHEET_REFERENCE_PATTERNS, chat_text)
    cross_sheet_signal = _has_pattern_match(_CROSS_SHEET_ANALYSIS_PATTERNS, chat_text)
    has_multi_sheet_context = len(mentioned) >= 2 or multi_sheet_signal or cross_sheet_signal

    if cross_sheet_signal and has_multi_sheet_context:
        return "multi_sheet_out_of_scope", "cross_sheet_join_not_supported", mentioned_payload

    if has_multi_sheet_context:
        return "multi_sheet_detected", "multi_sheet_query_detected", mentioned_payload

    return "single_sheet_in_scope", "single_sheet_query", mentioned_payload


def _decomposition_hint(
    *,
    locale: str,
    boundary_status: str,
    mentioned_sheets: list[dict[str, Any]],
) -> str:
    if boundary_status not in {"multi_sheet_detected", "multi_sheet_out_of_scope"}:
        return ""

    mentioned_names = [str(item.get("sheet_name") or "").strip() for item in mentioned_sheets if str(item.get("sheet_name") or "").strip()]
    first_name = mentioned_names[0] if mentioned_names else ""
    second_name = mentioned_names[1] if len(mentioned_names) > 1 else ""
    if first_name and second_name:
        return _localized_text(
            locale,
            zh=f"建议拆解：先分析「{first_name}」，再分析「{second_name}」。",
            en=f"Suggested decomposition: analyze '{first_name}' first, then '{second_name}'.",
            ja=f"推奨分解: まず「{first_name}」を分析し、次に「{second_name}」を分析してください。",
        )
    return _localized_text(
        locale,
        zh="建议拆解：先选择一个 sheet 完成分析，再继续问另一个 sheet。",
        en="Suggested decomposition: pick one sheet first, then continue with another sheet.",
        ja="推奨分解: まず 1 つのシートを選んで分析し、その後に別シートへ進んでください。",
    )


def _build_candidate_scores(
    workbook: WorkbookContext,
    *,
    chat_text: str,
    requested_sheet_index: int,
    followup_context: dict[str, Any] | None,
) -> list[dict[str, Any]]:
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
    return candidate_scores


def _multi_sheet_options(
    workbook: WorkbookContext,
    *,
    mentioned_sheets: list[dict[str, Any]],
    candidate_scores: list[dict[str, Any]],
) -> list[WorkbookSheetProfile]:
    options_by_index: dict[int, WorkbookSheetProfile] = {}

    for item in mentioned_sheets:
        sheet = _sheet_by_index(workbook, int(item.get("sheet_index") or 0))
        if sheet is not None:
            options_by_index[int(sheet.sheet_index)] = sheet

    if not options_by_index:
        for candidate in candidate_scores[: min(4, len(workbook.sheets))]:
            sheet = _sheet_by_index(workbook, int(candidate.get("sheet_index") or 0))
            if sheet is not None:
                options_by_index[int(sheet.sheet_index)] = sheet

    if not options_by_index:
        for sheet in workbook.sheets[: min(4, len(workbook.sheets))]:
            options_by_index[int(sheet.sheet_index)] = sheet

    return [options_by_index[index] for index in sorted(options_by_index)]


def _build_multi_sheet_clarification(
    workbook: WorkbookContext,
    *,
    locale: str,
    mentioned_sheets: list[dict[str, Any]],
    candidate_scores: list[dict[str, Any]],
    boundary_status: str,
) -> Clarification:
    is_out_of_scope = boundary_status == "multi_sheet_out_of_scope"
    reason = _localized_text(
        locale,
        zh=(
            "检测到你在同一问题中提到了多个工作表。该请求超出当前受控 Join Beta 范围。"
            "Join Beta 当前仅支持两表、单键、inner/left、聚合类问题（sum/count/avg/top/trend）。"
            "请先选择一个 sheet 开始，我可以再继续分析另一个。"
            if is_out_of_scope
            else "检测到这是多 sheet 问题。请先选择一个 sheet 开始，我可以再继续分析另一个。"
        ),
        en=(
            "Detected references to multiple sheets in one question. This request exceeds the controlled Join Beta scope. "
            "Join Beta currently supports two-sheet, single-key, inner/left joins for aggregate questions (sum/count/avg/top/trend). "
            "Choose one sheet to start, and I can continue with another sheet next."
            if is_out_of_scope
            else "Detected a multi-sheet question. Choose one sheet to start, and I can continue with another sheet next."
        ),
        ja=(
            "1つの質問で複数シートが参照されています。この要求は現在の制御付き Join Beta 範囲外です。"
            "Join Beta は 2 シート・単一キー・inner/left・集計系質問（sum/count/avg/top/trend）のみ対応します。"
            "まず 1 つ選んで開始し、その後に別シートを続けて分析できます。"
            if is_out_of_scope
            else "複数シートの質問を検出しました。まず 1 つ選んで開始し、その後に別シートを続けて分析できます。"
        ),
    )

    options = _multi_sheet_options(workbook, mentioned_sheets=mentioned_sheets, candidate_scores=candidate_scores)
    return Clarification(
        reason=reason,
        field=_localized_text(locale, zh="sheet", en="sheet", ja="sheet"),
        options=[
            {
                "label": f"{sheet.sheet_name} (Sheet {sheet.sheet_index})",
                "value": sheet.sheet_name,
                "sheet_index": sheet.sheet_index,
                "description": _localized_text(
                    locale,
                    zh=f"先从「{sheet.sheet_name}」开始分析。",
                    en=f"Start with '{sheet.sheet_name}' first.",
                    ja=f"まず「{sheet.sheet_name}」から分析します。",
                ),
            }
            for sheet in options
        ],
    )


def _followup_switch_requested(chat_text: str, followup_context: dict[str, Any] | None) -> bool:
    if not isinstance(followup_context, dict) or not bool(followup_context.get("is_followup")):
        return False
    hint = _followup_sheet_reference_hint(chat_text, followup_context)
    if hint in {"another", "previous"}:
        return True
    if bool(followup_context.get("wants_sheet_switch")):
        return True
    return _has_pattern_match(_FOLLOWUP_SHEET_SWITCH_PATTERNS, chat_text)


def _followup_sheet_reference_hint(chat_text: str, followup_context: dict[str, Any] | None) -> str:
    if isinstance(followup_context, dict):
        explicit_hint = str(followup_context.get("sheet_reference_hint") or "").strip().lower()
        if explicit_hint in {"another", "previous", "current"}:
            return explicit_hint
        if bool(followup_context.get("wants_previous_sheet")):
            return "previous"
        if bool(followup_context.get("wants_current_sheet")):
            return "current"
    if _has_pattern_match(_FOLLOWUP_PREVIOUS_SHEET_PATTERNS, chat_text):
        return "previous"
    if _has_pattern_match(_FOLLOWUP_CURRENT_SHEET_PATTERNS, chat_text):
        return "current"
    if _has_pattern_match(_FOLLOWUP_SHEET_SWITCH_PATTERNS, chat_text):
        return "another"
    return "auto"


def _previous_sheet_from_context(workbook: WorkbookContext, followup_context: dict[str, Any] | None) -> WorkbookSheetProfile | None:
    if not isinstance(followup_context, dict):
        return None
    previous_sheet_index = int(followup_context.get("previous_sheet_index") or 0)
    if previous_sheet_index > 0:
        previous = _sheet_by_index(workbook, previous_sheet_index)
        if previous is not None:
            return previous
    trajectory = followup_context.get("recent_sheet_trajectory")
    if isinstance(trajectory, list) and len(trajectory) >= 2:
        previous_payload = trajectory[-2] if isinstance(trajectory[-2], dict) else {}
        previous_sheet_index = int(previous_payload.get("sheet_index") or 0)
        if previous_sheet_index > 0:
            return _sheet_by_index(workbook, previous_sheet_index)
    return None


def _followup_switch_target_sheet(
    workbook: WorkbookContext,
    *,
    chat_text: str,
    followup_context: dict[str, Any] | None,
) -> WorkbookSheetProfile | None:
    reference_hint = _followup_sheet_reference_hint(chat_text, followup_context)
    if reference_hint == "previous":
        previous_sheet = _previous_sheet_from_context(workbook, followup_context)
        if previous_sheet is not None:
            return previous_sheet
        return None
    if not isinstance(followup_context, dict):
        return None
    last_sheet_index = int(followup_context.get("last_sheet_index") or 0)
    if last_sheet_index <= 0:
        return None
    if len(workbook.sheets) == 2:
        return next((sheet for sheet in workbook.sheets if int(sheet.sheet_index) != last_sheet_index), None)

    raw_visited = followup_context.get("visited_sheets")
    visited_indices = {
        int(item.get("sheet_index") or 0)
        for item in (raw_visited or [])
        if isinstance(item, dict) and int(item.get("sheet_index") or 0) > 0
    }
    unvisited = [
        sheet
        for sheet in workbook.sheets
        if int(sheet.sheet_index) not in visited_indices and int(sheet.sheet_index) != last_sheet_index
    ]
    if len(unvisited) == 1:
        return unvisited[0]
    return None


def _followup_switch_options(
    workbook: WorkbookContext,
    *,
    chat_text: str,
    followup_context: dict[str, Any] | None,
    candidate_scores: list[dict[str, Any]],
) -> list[WorkbookSheetProfile]:
    last_sheet_index = int((followup_context or {}).get("last_sheet_index") or 0) if isinstance(followup_context, dict) else 0
    reference_hint = _followup_sheet_reference_hint(chat_text, followup_context)
    ordered_indexes: list[int] = []

    if reference_hint == "previous":
        previous_sheet = _previous_sheet_from_context(workbook, followup_context)
        if previous_sheet is not None:
            return [previous_sheet]

    for item in candidate_scores:
        sheet_index = int(item.get("sheet_index") or 0)
        if sheet_index <= 0 or sheet_index == last_sheet_index or sheet_index in ordered_indexes:
            continue
        ordered_indexes.append(sheet_index)

    for sheet in workbook.sheets:
        sheet_index = int(sheet.sheet_index)
        if sheet_index == last_sheet_index or sheet_index in ordered_indexes:
            continue
        ordered_indexes.append(sheet_index)

    options: list[WorkbookSheetProfile] = []
    for sheet_index in ordered_indexes[: min(4, len(workbook.sheets))]:
        sheet = _sheet_by_index(workbook, sheet_index)
        if sheet is not None:
            options.append(sheet)
    return options


def _build_followup_switch_clarification(
    workbook: WorkbookContext,
    *,
    chat_text: str,
    locale: str,
    followup_context: dict[str, Any] | None,
    candidate_scores: list[dict[str, Any]],
) -> Clarification:
    reference_hint = _followup_sheet_reference_hint(chat_text, followup_context)
    options = _followup_switch_options(
        workbook,
        chat_text=chat_text,
        followup_context=followup_context,
        candidate_scores=candidate_scores,
    )
    return Clarification(
        reason=_localized_text(
            locale,
            zh=(
                "你提到要回到上一个 sheet。请先确认要切换到哪个 sheet，我会沿用上一轮分析口径继续。"
                if reference_hint == "previous"
                else "你提到要看另一个 sheet。请先选择要切换到哪个 sheet，我会沿用上一轮分析口径继续。"
            ),
            en=(
                "You asked to go back to a previous sheet. Choose the target sheet and I will continue with the previous analysis context."
                if reference_hint == "previous"
                else "You asked to continue on another sheet. Choose the next sheet and I will continue with the previous analysis context."
            ),
            ja=(
                "前のシートに戻る指定がありました。対象シートを選択してください。前ターンの分析文脈を引き継ぎます。"
                if reference_hint == "previous"
                else "別シートで続ける指定がありました。次に分析するシートを選択してください。前ターンの分析文脈を引き継ぎます。"
            ),
        ),
        field=_localized_text(locale, zh="sheet", en="sheet", ja="sheet"),
        options=[
            {
                "label": f"{sheet.sheet_name} (Sheet {sheet.sheet_index})",
                "value": sheet.sheet_name,
                "sheet_index": sheet.sheet_index,
                "description": _localized_text(
                    locale,
                    zh=f"切换到「{sheet.sheet_name}」并继续分析。",
                    en=f"Switch to '{sheet.sheet_name}' and continue.",
                    ja=f"「{sheet.sheet_name}」に切り替えて続行します。",
                ),
            }
            for sheet in options
        ],
    )


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
            ja="この質問は複数のシートに該当する可能性があります。先に対象シートを選択してください。",
        ),
        field=_localized_text(locale, zh="sheet", en="sheet", ja="sheet"),
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
    boundary_status, boundary_reason, mentioned_sheets = _boundary_metadata(workbook, chat_text=chat_text)
    decomposition_hint = _decomposition_hint(
        locale=locale,
        boundary_status=boundary_status,
        mentioned_sheets=mentioned_sheets,
    )
    boundary_kwargs = {
        "boundary_status": boundary_status,
        "boundary_reason": boundary_reason,
        "decomposition_hint": decomposition_hint,
        "mentioned_sheets": mentioned_sheets,
    }

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
            **boundary_kwargs,
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
            **boundary_kwargs,
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
            **boundary_kwargs,
        )

    explicit_sheet = _explicit_sheet_reference(workbook, chat_text)
    if _followup_switch_requested(chat_text, followup_context) and len(mentioned_sheets) <= 1:
        switch_hint = _followup_sheet_reference_hint(chat_text, followup_context)
        if explicit_sheet is not None:
            return SheetRoutingDecision(
                status="resolved",
                requested_sheet_index=requested_sheet_index,
                resolved_sheet_index=explicit_sheet.sheet_index,
                resolved_sheet_name=explicit_sheet.sheet_name,
                reason="followup_switch_to_explicit_sheet",
                matched_by="followup",
                confidence=1.0,
                **boundary_kwargs,
            )

        switch_target = _followup_switch_target_sheet(
            workbook,
            chat_text=chat_text,
            followup_context=followup_context,
        )
        if switch_target is not None:
            return SheetRoutingDecision(
                status="resolved",
                requested_sheet_index=requested_sheet_index,
                resolved_sheet_index=switch_target.sheet_index,
                resolved_sheet_name=switch_target.sheet_name,
                reason="followup_switch_to_previous_sheet" if switch_hint == "previous" else "followup_switch_to_another_sheet",
                matched_by="followup",
                confidence=0.9,
                **boundary_kwargs,
            )

        candidate_scores = _build_candidate_scores(
            workbook,
            chat_text=chat_text,
            requested_sheet_index=requested_sheet_index,
            followup_context=followup_context,
        )
        return SheetRoutingDecision(
            status="clarification",
            requested_sheet_index=requested_sheet_index,
            resolved_sheet_index=None,
            resolved_sheet_name="",
            reason="followup_sheet_switch_clarification",
            matched_by="followup",
            confidence=0.0,
            clarification=_build_followup_switch_clarification(
                workbook,
                chat_text=chat_text,
                locale=locale,
                followup_context=followup_context,
                candidate_scores=candidate_scores,
            ),
            candidate_scores=candidate_scores,
            **boundary_kwargs,
        )

    multi_sheet_signal = _has_pattern_match(_MULTI_SHEET_REFERENCE_PATTERNS, chat_text)
    should_force_multi_sheet_clarification = boundary_status in {"multi_sheet_detected", "multi_sheet_out_of_scope"} and (
        len(mentioned_sheets) >= 2 or multi_sheet_signal
    )
    if should_force_multi_sheet_clarification:
        candidate_scores = _build_candidate_scores(
            workbook,
            chat_text=chat_text,
            requested_sheet_index=requested_sheet_index,
            followup_context=followup_context,
        )
        return SheetRoutingDecision(
            status="clarification",
            requested_sheet_index=requested_sheet_index,
            resolved_sheet_index=None,
            resolved_sheet_name="",
            reason="multi_sheet_needs_decomposition",
            matched_by="multi_sheet_boundary",
            confidence=0.0,
            clarification=_build_multi_sheet_clarification(
                workbook,
                locale=locale,
                mentioned_sheets=mentioned_sheets,
                candidate_scores=candidate_scores,
                boundary_status=boundary_status,
            ),
            candidate_scores=candidate_scores,
            **boundary_kwargs,
        )
    if explicit_sheet is not None:
        return SheetRoutingDecision(
            status="resolved",
            requested_sheet_index=requested_sheet_index,
            resolved_sheet_index=explicit_sheet.sheet_index,
            resolved_sheet_name=explicit_sheet.sheet_name,
            reason="explicit_sheet_reference",
            matched_by="explicit_reference",
            confidence=1.0,
            **boundary_kwargs,
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
                **boundary_kwargs,
            )

    candidate_scores = _build_candidate_scores(
        workbook,
        chat_text=chat_text,
        requested_sheet_index=requested_sheet_index,
        followup_context=followup_context,
    )
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
            **boundary_kwargs,
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
            **boundary_kwargs,
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
        **boundary_kwargs,
    )
