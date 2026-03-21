from __future__ import annotations

from collections import Counter
import json
import logging
from pathlib import Path
from threading import Lock
from time import perf_counter
from typing import AsyncGenerator

from app.observability import log_event, log_task_step_event, new_request_id
from app.config import get_settings
from .core.i18n import t
from .analysis import analyze
from .analysis.utils import build_execution_disclosure
from .conversation.conversation_memory import build_turn_summary, conversation_store
from .pipeline import HEADER_HEALTH_ATTR, HEADER_PLAN_ATTR, load_dataframe, load_full_dataframe, read_workbook_context
from .routing.sheet_router import route_sheet


logger = logging.getLogger(__name__)
_MULTI_SHEET_FAILURE_REASON_COUNTER: Counter[str] = Counter()
_MULTI_SHEET_FAILURE_REASON_LOCK = Lock()
_MULTI_SHEET_FAILURE_REASON_LIMIT = 5
_FOLLOWUP_ACTION_CONTINUE_NEXT_STEP = "continue_next_step"


def _sse(payload: dict) -> str:
    return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"


def _cache_token(path: Path, *, sheet_index: int, scope: str, limit: int | None = None) -> str:
    resolved = path.resolve()
    stat = resolved.stat()
    return "|".join(
        [
            str(resolved),
            str(int(stat.st_mtime_ns)),
            str(int(stat.st_size)),
            str(int(sheet_index or 1)),
            scope,
            "" if limit is None else str(int(limit)),
        ]
    )


def _sheet_routing_explanation(*, reason: str, matched_by: str, locale: str) -> str:
    normalized_locale = str(locale or "").lower()
    use_zh = normalized_locale.startswith("zh")
    use_ja = normalized_locale.startswith("ja")
    if reason == "explicit_sheet_reference":
        return "问题里显式提到了该 sheet。" if use_zh else ("質問でこのシートが明示されました。" if use_ja else "The question explicitly mentioned this sheet.")
    if reason == "clarification_resolution":
        return "使用了你在澄清步骤里确认的 sheet。" if use_zh else ("確認ステップで選択したシートを使用しました。" if use_ja else "Used the sheet you selected in clarification.")
    if reason == "manual_sheet_override":
        return "使用了你手动选择的 sheet。" if use_zh else ("手動で選択したシートを使用しました。" if use_ja else "Used your manual sheet selection.")
    if reason == "followup_inherit_previous_sheet":
        return "沿用了上一轮分析所在的 sheet。" if use_zh else ("前ターンと同じシートを引き継ぎました。" if use_ja else "Inherited the sheet from the previous turn.")
    if reason == "followup_switch_to_another_sheet":
        return "根据追问切换到了另一个 sheet。" if use_zh else ("フォローアップ要求により別シートへ切り替えました。" if use_ja else "Switched to another sheet based on the follow-up request.")
    if reason == "followup_switch_to_previous_sheet":
        return "根据追问切换回上一个 sheet。" if use_zh else ("フォローアップ要求により前のシートへ戻しました。" if use_ja else "Switched back to the previous sheet based on the follow-up request.")
    if reason == "followup_switch_to_explicit_sheet":
        return "根据追问中明确指定的 sheet 进行了切换。" if use_zh else ("フォローアップで明示されたシートへ切り替えました。" if use_ja else "Switched to the sheet explicitly mentioned in the follow-up.")
    if reason == "auto_routed_by_sheet_profile":
        return "根据问题内容和字段匹配信号自动命中了该 sheet。" if use_zh else ("質問内容と列マッチ信号に基づいて自動ルーティングしました。" if use_ja else "Auto-routed to this sheet based on question and column-match signals.")
    if reason == "ambiguous_sheet_match":
        return "多个候选 sheet 评分接近，需要你确认。" if use_zh else ("複数シートのスコアが近いため確認が必要です。" if use_ja else "Multiple sheets had close scores, so clarification is required.")
    if reason == "requested_sheet_default":
        return "没有足够命中信号，按请求 sheet 兜底。" if use_zh else ("十分な一致信号がないため要求シートへフォールバックしました。" if use_ja else "No strong routing signal found; fell back to the requested sheet.")
    if reason == "multi_sheet_needs_decomposition":
        return "识别到多 sheet 问题，需先选择一个 sheet 顺序分析。" if use_zh else ("複数シート質問を検出したため、先に 1 シートを選択してください。" if use_ja else "Detected a multi-sheet question; choose one sheet first for sequential analysis.")
    if reason == "single_sheet_workbook":
        return "当前工作簿只有一个 sheet，直接在该 sheet 上执行。" if use_zh else ("単一シートのワークブックのため、そのまま実行しました。" if use_ja else "Workbook has a single sheet, so it was used directly.")
    if matched_by == "auto_routing":
        return "根据字段与问题语义匹配进行了自动路由。" if use_zh else ("列と質問の意味一致で自動ルーティングしました。" if use_ja else "Applied auto-routing from column and question semantics.")
    return ""


def _sheet_routing_payload(workbook_context: object, routing_decision: object, *, locale: str) -> dict[str, object]:
    sheet_count = len(getattr(workbook_context, "sheets", []) or [])
    payload = dict(routing_decision.model_dump()) if hasattr(routing_decision, "model_dump") else dict(routing_decision)
    payload["workbook_sheet_count"] = sheet_count
    reason = str(payload.get("reason") or "")
    matched_by = str(payload.get("matched_by") or "")
    payload["explanation_code"] = reason or matched_by or "unknown"
    payload["explanation"] = _sheet_routing_explanation(reason=reason, matched_by=matched_by, locale=locale)
    return payload


def _sheet_name_for_index(workbook_context: object, sheet_index: int) -> str:
    for sheet in getattr(workbook_context, "sheets", []) or []:
        if int(getattr(sheet, "sheet_index", 0) or 0) == int(sheet_index or 0):
            return str(getattr(sheet, "sheet_name", "") or "")
    return ""


def _sheet_name_map(workbook_context: object) -> dict[int, str]:
    mapping: dict[int, str] = {}
    for sheet in getattr(workbook_context, "sheets", []) or []:
        sheet_index = int(getattr(sheet, "sheet_index", 0) or 0)
        if sheet_index <= 0:
            continue
        mapping[sheet_index] = str(getattr(sheet, "sheet_name", "") or "")
    return mapping


def _visited_sheets_from_followup(followup_context: dict[str, object] | None) -> list[dict[str, object]]:
    if not isinstance(followup_context, dict):
        return []
    raw = followup_context.get("visited_sheets")
    if not isinstance(raw, list):
        return []
    visited: list[dict[str, object]] = []
    seen: set[int] = set()
    for item in raw:
        if not isinstance(item, dict):
            continue
        sheet_index = int(item.get("sheet_index") or 0)
        if sheet_index <= 0 or sheet_index in seen:
            continue
        seen.add(sheet_index)
        visited.append(
            {
                "sheet_index": sheet_index,
                "sheet_name": str(item.get("sheet_name") or ""),
            }
        )
    return visited


def _build_sheet_sequence_payload(
    *,
    followup_context: dict[str, object] | None,
    resolved_sheet_index: int | None,
    resolved_sheet_name: str,
    routing_reason: str,
) -> dict[str, object]:
    previous_sheet_index = int((followup_context or {}).get("last_sheet_index") or 0) if isinstance(followup_context, dict) else 0
    previous_sheet_name = str((followup_context or {}).get("last_sheet_name") or "") if isinstance(followup_context, dict) else ""
    switched_from_previous = bool(
        previous_sheet_index
        and resolved_sheet_index
        and int(previous_sheet_index) != int(resolved_sheet_index)
    )

    visited = _visited_sheets_from_followup(followup_context)
    if resolved_sheet_index:
        resolved_index = int(resolved_sheet_index)
        if all(int(item.get("sheet_index") or 0) != resolved_index for item in visited):
            visited.append({"sheet_index": resolved_index, "sheet_name": str(resolved_sheet_name or "")})

    last_sheet_switch_reason = str(
        routing_reason
        if switched_from_previous and routing_reason
        else ((followup_context or {}).get("last_sheet_switch_reason") or "")
    )
    return {
        "previous_sheet_index": previous_sheet_index or None,
        "previous_sheet_name": previous_sheet_name,
        "switched_from_previous": switched_from_previous,
        "last_sheet_switch_reason": last_sheet_switch_reason,
        "visited_sheets": visited,
    }


def _build_task_steps_payload(
    *,
    workbook_context: object,
    routing_decision: object,
    sheet_sequence: dict[str, object],
    resolved_sheet_index: int | None,
) -> dict[str, object]:
    sheet_count = len(getattr(workbook_context, "sheets", []) or [])
    if sheet_count <= 1:
        return {"task_steps": [], "current_step_id": ""}

    status = str(getattr(routing_decision, "status", "") or "")
    reason = str(getattr(routing_decision, "reason", "") or "")
    boundary_status = str(getattr(routing_decision, "boundary_status", "") or "")
    relevant_boundary = boundary_status in {"multi_sheet_detected", "multi_sheet_out_of_scope"}
    visited = sheet_sequence.get("visited_sheets")
    visited_sheets = [item for item in visited if isinstance(item, dict)] if isinstance(visited, list) else []
    should_render = (
        relevant_boundary
        or reason.startswith("followup_switch_to_")
        or reason == "followup_sheet_switch_clarification"
        or len(visited_sheets) >= 2
    )
    if not should_render:
        return {"task_steps": [], "current_step_id": ""}

    name_map = _sheet_name_map(workbook_context)
    ordered_indexes: list[int] = []
    ordered_names: dict[int, str] = {}

    def add_step(sheet_index: int, sheet_name: str = "") -> None:
        normalized_index = int(sheet_index or 0)
        if normalized_index <= 0 or normalized_index in ordered_indexes:
            return
        ordered_indexes.append(normalized_index)
        ordered_names[normalized_index] = str(sheet_name or "").strip()

    mentioned_sheets = getattr(routing_decision, "mentioned_sheets", None)
    if isinstance(mentioned_sheets, list):
        for item in mentioned_sheets:
            if not isinstance(item, dict):
                continue
            add_step(int(item.get("sheet_index") or 0), str(item.get("sheet_name") or ""))

    clarification = getattr(routing_decision, "clarification", None)
    clarification_options = getattr(clarification, "options", None)
    if isinstance(clarification_options, list) and reason in {"multi_sheet_needs_decomposition", "followup_sheet_switch_clarification"}:
        for option in clarification_options:
            if not isinstance(option, dict):
                continue
            option_sheet_index = int(option.get("sheet_index") or 0)
            option_sheet_name = str(option.get("value") or option.get("label") or "")
            add_step(option_sheet_index, option_sheet_name)

    for item in visited_sheets:
        add_step(int(item.get("sheet_index") or 0), str(item.get("sheet_name") or ""))

    if resolved_sheet_index:
        add_step(int(resolved_sheet_index), "")

    if not ordered_indexes and relevant_boundary:
        for sheet in getattr(workbook_context, "sheets", [])[: min(2, sheet_count)] or []:
            add_step(int(getattr(sheet, "sheet_index", 0) or 0), str(getattr(sheet, "sheet_name", "") or ""))

    visited_indexes = {int(item.get("sheet_index") or 0) for item in visited_sheets}
    task_steps: list[dict[str, object]] = []
    current_step_id = ""
    for index in ordered_indexes:
        step_id = f"sheet-{index}"
        status_value = "pending"
        if resolved_sheet_index and int(index) == int(resolved_sheet_index):
            status_value = "current"
            current_step_id = step_id
        elif int(index) in visited_indexes:
            status_value = "completed"
        task_steps.append(
            {
                "step_id": step_id,
                "sheet_index": index,
                "sheet_name": ordered_names.get(index) or name_map.get(index) or "",
                "status": status_value,
            }
        )

    if not current_step_id and status == "clarification" and task_steps:
        task_steps[0]["status"] = "current"
        current_step_id = str(task_steps[0].get("step_id") or "")

    return {
        "task_steps": task_steps,
        "current_step_id": current_step_id,
    }


def _normalize_followup_action(value: str | None) -> str:
    normalized = str(value or "").strip()
    if normalized == _FOLLOWUP_ACTION_CONTINUE_NEXT_STEP:
        return normalized
    return ""


def _normalize_step_status(value: str) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"pending", "current", "completed", "failed"}:
        return normalized
    return "pending"


def _task_steps_from_followup_context(followup_context: dict[str, object] | None) -> list[dict[str, object]]:
    if not isinstance(followup_context, dict):
        return []
    raw_steps = followup_context.get("last_task_steps")
    if not isinstance(raw_steps, list):
        return []
    steps: list[dict[str, object]] = []
    for item in raw_steps:
        if not isinstance(item, dict):
            continue
        sheet_index = int(item.get("sheet_index") or 0)
        if sheet_index <= 0:
            continue
        step_id = str(item.get("step_id") or "").strip() or f"sheet-{sheet_index}"
        steps.append(
            {
                "step_id": step_id,
                "sheet_index": sheet_index,
                "sheet_name": str(item.get("sheet_name") or "").strip(),
                "status": _normalize_step_status(str(item.get("status") or "")),
            }
        )
    return steps


def _resolve_continue_next_step_target(followup_context: dict[str, object] | None) -> dict[str, object] | None:
    steps = _task_steps_from_followup_context(followup_context)
    if len(steps) <= 1:
        return None

    current_step_id = str((followup_context or {}).get("last_current_step_id") or "").strip() if isinstance(followup_context, dict) else ""
    current_index = -1
    if current_step_id:
        current_index = next((index for index, step in enumerate(steps) if str(step.get("step_id") or "") == current_step_id), -1)

    target: dict[str, object] | None = None
    if current_index >= 0 and current_index + 1 < len(steps):
        candidate = steps[current_index + 1]
        candidate_status = str(candidate.get("status") or "")
        if candidate_status in {"pending", "failed"}:
            target = candidate

    if target is None:
        target = next((step for step in steps if str(step.get("status") or "") in {"pending", "failed"}), None)

    if target is None:
        return None

    target_sheet_index = int(target.get("sheet_index") or 0)
    if target_sheet_index <= 0:
        return None
    target_sheet_name = str(target.get("sheet_name") or "").strip()
    return {
        "sheet_index": target_sheet_index,
        "sheet_name": target_sheet_name,
    }


def _followup_context_with_continue_next_step_resolution(
    followup_context: dict[str, object] | None,
    *,
    target_sheet_index: int,
    target_sheet_name: str,
) -> dict[str, object]:
    base = dict(followup_context) if isinstance(followup_context, dict) else {}
    selected_value = str(target_sheet_name or "").strip() or str(int(target_sheet_index))
    base["clarification_resolution"] = {
        "kind": "sheet_resolution",
        "selected_value": selected_value,
    }
    return base


def _find_task_step_by_id(steps: list[dict[str, object]], step_id: str) -> dict[str, object] | None:
    normalized_step_id = str(step_id or "").strip()
    if not normalized_step_id:
        return None
    for step in steps:
        if not isinstance(step, dict):
            continue
        if str(step.get("step_id") or "").strip() == normalized_step_id:
            return step
    return None


def _task_step_event_payload(step: dict[str, object] | None, *, step_id: str = "") -> dict[str, object]:
    if not isinstance(step, dict):
        return {}
    payload_step_id = str(step_id or step.get("step_id") or "").strip()
    sheet_index = int(step.get("sheet_index") or 0)
    if not payload_step_id or sheet_index <= 0:
        return {}
    return {
        "step_id": payload_step_id,
        "step_status": _normalize_step_status(str(step.get("status") or "")),
        "sheet_index": sheet_index,
        "sheet_name": str(step.get("sheet_name") or "").strip(),
    }


def _build_task_step_transition_events(
    *,
    previous_steps: list[dict[str, object]],
    previous_current_step_id: str,
    current_steps: list[dict[str, object]],
    current_step_id: str,
) -> list[dict[str, object]]:
    events: list[dict[str, object]] = []
    normalized_previous_current = str(previous_current_step_id or "").strip()
    normalized_current = str(current_step_id or "").strip()

    if normalized_previous_current and normalized_previous_current != normalized_current:
        completed_step = _find_task_step_by_id(current_steps, normalized_previous_current)
        completed_payload = _task_step_event_payload(completed_step, step_id=normalized_previous_current)
        if completed_payload and str(completed_payload.get("step_status") or "") == "completed":
            events.append(
                {
                    "event": "task_step_completed",
                    **completed_payload,
                }
            )

    if normalized_current and normalized_current != normalized_previous_current:
        started_step = _find_task_step_by_id(current_steps, normalized_current)
        if started_step is None:
            started_step = _find_task_step_by_id(previous_steps, normalized_current)
        started_payload = _task_step_event_payload(started_step, step_id=normalized_current)
        if started_payload and str(started_payload.get("step_status") or "") == "current":
            events.append(
                {
                    "event": "task_step_started",
                    **started_payload,
                }
            )

    return events


def _emit_task_step_transition_events(
    *,
    request_id: str,
    file_id: str,
    conversation_id: str,
    followup_action: str,
    previous_steps: list[dict[str, object]],
    previous_current_step_id: str,
    current_steps: list[dict[str, object]],
    current_step_id: str,
) -> list[dict[str, object]]:
    events = _build_task_step_transition_events(
        previous_steps=previous_steps,
        previous_current_step_id=previous_current_step_id,
        current_steps=current_steps,
        current_step_id=current_step_id,
    )
    for item in events:
        log_task_step_event(
            logger,
            event=str(item.get("event") or ""),
            request_id=request_id,
            file_id=file_id,
            conversation_id=conversation_id,
            step_id=str(item.get("step_id") or ""),
            step_status=str(item.get("step_status") or ""),
            sheet_index=int(item.get("sheet_index") or 0),
            sheet_name=str(item.get("sheet_name") or ""),
            followup_action=followup_action,
        )
    return events


def _emit_task_step_failed_event(
    *,
    request_id: str,
    file_id: str,
    conversation_id: str,
    followup_action: str,
    current_steps: list[dict[str, object]],
    current_step_id: str,
    error_type: str,
) -> dict[str, object] | None:
    normalized_current = str(current_step_id or "").strip()
    failed_step = _find_task_step_by_id(current_steps, normalized_current) if normalized_current else None
    if failed_step is None:
        failed_step = next(
            (item for item in current_steps if _normalize_step_status(str(item.get("status") or "")) == "current"),
            None,
        )
    failed_payload = _task_step_event_payload(failed_step, step_id=normalized_current)
    if not failed_payload:
        return None
    failed_payload["step_status"] = "failed"
    log_task_step_event(
        logger,
        event="task_step_failed",
        request_id=request_id,
        file_id=file_id,
        conversation_id=conversation_id,
        step_id=str(failed_payload.get("step_id") or ""),
        step_status="failed",
        sheet_index=int(failed_payload.get("sheet_index") or 0),
        sheet_name=str(failed_payload.get("sheet_name") or ""),
        followup_action=followup_action,
        reason="analysis_exception",
        error_type=error_type,
    )
    return {
        "event": "task_step_failed",
        **failed_payload,
        "reason": "analysis_exception",
        "error_type": str(error_type or "").strip(),
    }


def _trim_step_summary(value: str, *, limit: int = 220) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)] + "..."


def _build_previous_step_summary(
    *,
    followup_context: dict[str, object] | None,
    resolved_sheet_index: int | None,
) -> dict[str, object] | None:
    if not isinstance(followup_context, dict):
        return None
    last_turn = followup_context.get("last_turn")
    if not isinstance(last_turn, dict):
        return None
    previous_sheet_index = int(last_turn.get("sheet_index") or 0)
    previous_sheet_name = str(last_turn.get("sheet_name") or "").strip()
    if previous_sheet_index <= 0:
        return None
    if resolved_sheet_index and int(resolved_sheet_index) == previous_sheet_index:
        return None

    pipeline_summary = last_turn.get("pipeline_summary")
    safe_pipeline_summary = dict(pipeline_summary) if isinstance(pipeline_summary, dict) else {}
    answer_summary = _trim_step_summary(
        str(last_turn.get("answer_summary") or "")
        or str(last_turn.get("analysis_text") or "")
        or str(last_turn.get("answer") or "")
    )
    result_row_count = int(last_turn.get("result_row_count") or safe_pipeline_summary.get("result_row_count") or 0)
    intent = str(last_turn.get("intent") or safe_pipeline_summary.get("intent") or "").strip()
    if not answer_summary and not intent and result_row_count <= 0:
        return None
    return {
        "sheet_index": previous_sheet_index,
        "sheet_name": previous_sheet_name,
        "answer_summary": answer_summary,
        "intent": intent,
        "result_row_count": result_row_count,
    }


def _build_step_comparison_payload(
    *,
    previous_step: dict[str, object] | None,
    current_sheet_index: int,
    current_sheet_name: str,
    current_answer_summary: str,
    current_intent: str,
    current_result_row_count: int,
) -> dict[str, object] | None:
    if not isinstance(previous_step, dict):
        return None
    previous_sheet_index = int(previous_step.get("sheet_index") or 0)
    if previous_sheet_index <= 0 or previous_sheet_index == int(current_sheet_index or 0):
        return None
    current_summary = _trim_step_summary(current_answer_summary)
    if not current_summary and not current_intent and int(current_result_row_count or 0) <= 0:
        return None
    current_step = {
        "sheet_index": int(current_sheet_index or 0),
        "sheet_name": str(current_sheet_name or "").strip(),
        "answer_summary": current_summary,
        "intent": str(current_intent or "").strip(),
        "result_row_count": int(current_result_row_count or 0),
    }
    if int(current_step["sheet_index"]) <= 0:
        return None
    return {
        "previous_step": previous_step,
        "current_step": current_step,
        "independent_scopes": True,
    }


def _resolve_multi_sheet_failure_reason(routing_decision: object) -> str:
    status = str(getattr(routing_decision, "status", "") or "")
    reason = str(getattr(routing_decision, "reason", "") or "")
    boundary_status = str(getattr(routing_decision, "boundary_status", "") or "")
    boundary_reason = str(getattr(routing_decision, "boundary_reason", "") or "")

    if status != "clarification":
        return ""
    if reason == "followup_sheet_switch_clarification":
        return reason
    if boundary_status in {"multi_sheet_detected", "multi_sheet_out_of_scope"}:
        if boundary_reason == "cross_sheet_join_not_supported":
            return boundary_reason
        if reason:
            return reason
        return boundary_reason
    return ""


def _record_multi_sheet_failure_reason(reason: str) -> dict[str, int]:
    normalized = str(reason or "").strip()
    if not normalized:
        return {}
    with _MULTI_SHEET_FAILURE_REASON_LOCK:
        _MULTI_SHEET_FAILURE_REASON_COUNTER[normalized] += 1
        return {
            str(item_reason): int(item_count)
            for item_reason, item_count in _MULTI_SHEET_FAILURE_REASON_COUNTER.most_common(_MULTI_SHEET_FAILURE_REASON_LIMIT)
        }


def _build_sheet_routing_observability(
    *,
    routing_decision: object,
    sheet_sequence: dict[str, object],
    followup_action: str,
    followup_action_target_sheet_index: int | None,
) -> dict[str, object]:
    boundary_status = str(getattr(routing_decision, "boundary_status", "") or "")
    clarification = getattr(routing_decision, "clarification", None)
    clarification_options = getattr(clarification, "options", None)
    clarification_sheet_count = len(clarification_options) if isinstance(clarification_options, list) else 0
    sheet_switch_count = 1 if bool(sheet_sequence.get("switched_from_previous")) else 0
    failure_reason = _resolve_multi_sheet_failure_reason(routing_decision)
    top_failure_reasons = _record_multi_sheet_failure_reason(failure_reason)
    return {
        "multi_sheet_detected": int(boundary_status in {"multi_sheet_detected", "multi_sheet_out_of_scope"}),
        "clarification_sheet_count": clarification_sheet_count,
        "sheet_switch_count": sheet_switch_count,
        "multi_sheet_failure_reason": failure_reason,
        "multi_sheet_top_failure_reasons": top_failure_reasons,
        "followup_action": followup_action,
        "followup_action_applied": int(bool(followup_action_target_sheet_index)),
        "followup_action_target_sheet_index": int(followup_action_target_sheet_index or 0),
    }


async def stream_spreadsheet_chat(
    *,
    path: Path,
    file_id: str,
    chat_text: str,
    mode: str,
    sheet_index: int,
    sheet_override: bool = False,
    locale: str,
    conversation_id: str | None = None,
    clarification_resolution: dict | None = None,
    followup_action: str | None = None,
    request_id: str | None = None,
) -> AsyncGenerator[str, None]:
    request_id = str(request_id or new_request_id())
    request_started_at = perf_counter()
    settings = get_settings()
    normalized_followup_action = ""
    active_conversation_id = str(conversation_id or "")
    task_step_state: dict[str, object] = {"task_steps": [], "current_step_id": ""}
    task_step_transition_events: list[dict[str, object]] = []
    task_step_started_count = 0
    task_step_completed_count = 0
    try:
        session, context_reset = conversation_store.ensure_session(
            conversation_id=conversation_id,
            file_id=file_id,
            sheet_index=sheet_index,
            locale=locale,
        )
        active_conversation_id = str(session.conversation_id or "")
        followup_context = conversation_store.build_followup_context(
            session,
            chat_text=chat_text,
            clarification_resolution=clarification_resolution,
        )
        normalized_followup_action = _normalize_followup_action(followup_action)
        followup_action_target = (
            _resolve_continue_next_step_target(followup_context)
            if normalized_followup_action == _FOLLOWUP_ACTION_CONTINUE_NEXT_STEP
            else None
        )
        workbook_context = read_workbook_context(
            path,
            file_id=file_id,
            active_sheet_index=sheet_index,
            preview_limit=min(8, int(settings.max_analysis_rows)),
        )
        routing_followup_context: dict[str, object] | None = followup_context
        if isinstance(followup_action_target, dict):
            routing_followup_context = _followup_context_with_continue_next_step_resolution(
                followup_context,
                target_sheet_index=int(followup_action_target.get("sheet_index") or 0),
                target_sheet_name=str(followup_action_target.get("sheet_name") or ""),
            )
        routing_decision = route_sheet(
            workbook_context,
            chat_text=chat_text,
            requested_sheet_index=sheet_index,
            requested_sheet_override=sheet_override,
            followup_context=routing_followup_context,
            locale=locale,
        )
        resolved_sheet_index = int(routing_decision.resolved_sheet_index or workbook_context.active_sheet_index or sheet_index or 1)
        resolved_sheet_name = str(routing_decision.resolved_sheet_name or workbook_context.active_sheet_name or "")
        sheet_sequence = _build_sheet_sequence_payload(
            followup_context=followup_context,
            resolved_sheet_index=resolved_sheet_index if routing_decision.status == "resolved" else None,
            resolved_sheet_name=resolved_sheet_name if routing_decision.status == "resolved" else "",
            routing_reason=str(routing_decision.reason or ""),
        )
        sheet_routing_observability = _build_sheet_routing_observability(
            routing_decision=routing_decision,
            sheet_sequence=sheet_sequence,
            followup_action=normalized_followup_action,
            followup_action_target_sheet_index=(
                int(followup_action_target.get("sheet_index") or 0)
                if isinstance(followup_action_target, dict)
                else None
            ),
        )
        task_step_state = _build_task_steps_payload(
            workbook_context=workbook_context,
            routing_decision=routing_decision,
            sheet_sequence=sheet_sequence,
            resolved_sheet_index=resolved_sheet_index if routing_decision.status == "resolved" else None,
        )
        previous_task_steps = _task_steps_from_followup_context(followup_context)
        previous_current_step_id = (
            str(followup_context.get("last_current_step_id") or "").strip() if isinstance(followup_context, dict) else ""
        )
        current_task_steps = (
            [item for item in task_step_state.get("task_steps", []) if isinstance(item, dict)]
            if isinstance(task_step_state.get("task_steps"), list)
            else []
        )
        current_task_step_id = str(task_step_state.get("current_step_id") or "").strip()
        task_step_transition_events = _emit_task_step_transition_events(
            request_id=request_id,
            file_id=file_id,
            conversation_id=active_conversation_id,
            followup_action=normalized_followup_action,
            previous_steps=previous_task_steps,
            previous_current_step_id=previous_current_step_id,
            current_steps=current_task_steps,
            current_step_id=current_task_step_id,
        )
        task_step_started_count = sum(1 for item in task_step_transition_events if str(item.get("event") or "") == "task_step_started")
        task_step_completed_count = sum(
            1 for item in task_step_transition_events if str(item.get("event") or "") == "task_step_completed"
        )
        planner_followup_context = (
            dict(routing_followup_context) if isinstance(routing_followup_context, dict) else routing_followup_context
        )
        if isinstance(planner_followup_context, dict):
            if normalized_followup_action:
                planner_followup_context["followup_action"] = normalized_followup_action
            if isinstance(followup_action_target, dict):
                planner_followup_context["wants_sheet_switch"] = True
                planner_followup_context["sheet_reference_hint"] = "another"
                planner_followup_context["followup_action_target_sheet_index"] = int(followup_action_target.get("sheet_index") or 0)
                planner_followup_context["followup_action_target_sheet_name"] = str(
                    followup_action_target.get("sheet_name") or ""
                )
            planner_followup_context["current_sheet_index"] = resolved_sheet_index
            planner_followup_context["current_sheet_name"] = resolved_sheet_name
            planner_followup_context["sheet_switched_from_previous"] = bool(sheet_sequence.get("switched_from_previous"))
            planner_followup_context["sheet_switch_reason"] = str(sheet_sequence.get("last_sheet_switch_reason") or "")
            planner_followup_context["task_steps"] = task_step_state.get("task_steps")
            planner_followup_context["current_step_id"] = task_step_state.get("current_step_id")
        followup_analysis_anchor = (
            dict(planner_followup_context.get("analysis_anchor"))
            if isinstance(planner_followup_context, dict) and isinstance(planner_followup_context.get("analysis_anchor"), dict)
            else None
        )
        analysis_anchor_reused = bool(
            isinstance(planner_followup_context, dict)
            and planner_followup_context.get("is_followup")
            and isinstance(followup_analysis_anchor, dict)
        )
        previous_step_summary = _build_previous_step_summary(
            followup_context=followup_context,
            resolved_sheet_index=resolved_sheet_index if routing_decision.status == "resolved" else None,
        )
        session.sheet_index = resolved_sheet_index
        session.sheet_name = resolved_sheet_name

        if routing_decision.status == "clarification" and routing_decision.clarification is not None:
            message = t(locale, "clarification", reason=routing_decision.clarification.reason)
            execution_disclosure = build_execution_disclosure(locale, rows_loaded=0, exact_used=False, fallback_reason=message)
            meta_event = {
                "request_id": request_id,
                "file_id": file_id,
                "conversation_id": session.conversation_id,
                "meta": {
                    "request_id": request_id,
                    "sheet_index": sheet_index,
                    "requested_sheet_name": _sheet_name_for_index(workbook_context, sheet_index),
                    "sheet_override": sheet_override,
                    "resolved_sheet_index": None,
                    "resolved_sheet_name": "",
                    "workbook_sheet_count": len(workbook_context.sheets),
                    "conversation_id": session.conversation_id,
                    "followup_turn_count": int(followup_context.get("turn_count") or 0) if followup_context else 0,
                    "followup_context_reset": context_reset,
                    "followup_action": normalized_followup_action or None,
                    "followup_action_target_sheet_index": (
                        int(followup_action_target.get("sheet_index") or 0)
                        if isinstance(followup_action_target, dict)
                        else None
                    ),
                    "analysis_anchor_reused": analysis_anchor_reused,
                    "analysis_anchor": followup_analysis_anchor,
                    "sheet_routing": _sheet_routing_payload(workbook_context, routing_decision, locale=locale),
                    "sheet_sequence": sheet_sequence,
                    "task_steps": task_step_state.get("task_steps"),
                    "current_step_id": task_step_state.get("current_step_id"),
                    "observability": {
                        "sheet_routing": sheet_routing_observability,
                        "task_steps": task_step_transition_events,
                    },
                },
            }
            yield _sse(meta_event)

            pipeline = {
                "status": "clarification",
                "clarification_stage": "sheet_routing",
                "sheet_routing": _sheet_routing_payload(workbook_context, routing_decision, locale=locale),
                "sheet_sequence": sheet_sequence,
                "task_steps": task_step_state.get("task_steps"),
                "current_step_id": task_step_state.get("current_step_id"),
                "followup_action": normalized_followup_action or None,
                "followup_action_target_sheet_index": (
                    int(followup_action_target.get("sheet_index") or 0)
                    if isinstance(followup_action_target, dict)
                    else None
                ),
                "analysis_anchor_reused": analysis_anchor_reused,
                "analysis_anchor": followup_analysis_anchor,
                "clarification": routing_decision.clarification.model_dump(),
                "workbook_sheets": [sheet.model_dump() for sheet in workbook_context.sheets],
                "observability": {
                    "request_id": request_id,
                    "request_total_ms": round((perf_counter() - request_started_at) * 1000, 3),
                    "task_step_started_count": task_step_started_count,
                    "task_step_completed_count": task_step_completed_count,
                    "task_step_events": task_step_transition_events,
                    **sheet_routing_observability,
                },
            }
            log_event(
                logger,
                "spreadsheet_chat_clarification",
                request_id=request_id,
                file_id=file_id,
                conversation_id=session.conversation_id,
                sheet_index=sheet_index,
                resolved_sheet_index=None,
                routing_reason=str(routing_decision.reason or ""),
                boundary_status=str(routing_decision.boundary_status or ""),
                multi_sheet_detected=sheet_routing_observability["multi_sheet_detected"],
                clarification_sheet_count=sheet_routing_observability["clarification_sheet_count"],
                sheet_switch_count=sheet_routing_observability["sheet_switch_count"],
                multi_sheet_failure_reason=sheet_routing_observability["multi_sheet_failure_reason"],
                multi_sheet_top_failure_reasons=sheet_routing_observability["multi_sheet_top_failure_reasons"],
                followup_action=normalized_followup_action,
                followup_action_applied=sheet_routing_observability["followup_action_applied"],
                followup_action_target_sheet_index=sheet_routing_observability["followup_action_target_sheet_index"],
                task_step_started_count=task_step_started_count,
                task_step_completed_count=task_step_completed_count,
            )
            yield _sse(
                {
                    "request_id": request_id,
                    "file_id": file_id,
                    "conversation_id": session.conversation_id,
                    "execution_disclosure": execution_disclosure.model_dump(),
                    "data_scope": execution_disclosure.data_scope,
                    "scope_text": execution_disclosure.scope_text,
                    "scope_warning": execution_disclosure.scope_warning,
                    "exact_used": execution_disclosure.exact_used,
                    "pipeline": pipeline,
                }
            )
            yield _sse(
                {
                    "request_id": request_id,
                    "file_id": file_id,
                    "conversation_id": session.conversation_id,
                    "mode": "text",
                    "answer": message,
                    "analysis_text": message,
                    "execution_disclosure": execution_disclosure.model_dump(),
                }
            )
            conversation_store.append_turn(
                session,
                build_turn_summary(
                    question=chat_text,
                    requested_mode=mode,
                    result_mode="text",
                    pipeline=pipeline,
                    answer=message,
                    analysis_text=message,
                    chart_spec=None,
                    execution_disclosure=execution_disclosure.model_dump(),
                ),
            )
            yield _sse({"request_id": request_id, "file_id": file_id, "conversation_id": session.conversation_id, "answer": "<|EOS|>"})
            return

        sampled_limit = int(settings.max_analysis_rows)
        sampled_cache_key = "analysis_sampled"
        sampled_cache_token = _cache_token(path, sheet_index=resolved_sheet_index, scope=sampled_cache_key, limit=sampled_limit)
        sampled_load_started_at = perf_counter()
        sampled_cached = conversation_store.get_cached_dataframe(
            session,
            cache_key=sampled_cache_key,
            cache_token=sampled_cache_token,
        )
        if sampled_cached is not None:
            df, sheet_name = sampled_cached
            sampled_cache_hit = True
        else:
            df, sheet_name = load_dataframe(path, sheet_index=resolved_sheet_index, limit=sampled_limit)
            conversation_store.set_cached_dataframe(
                session,
                cache_key=sampled_cache_key,
                cache_token=sampled_cache_token,
                dataframe=df,
                sheet_name=sheet_name,
            )
            sampled_cache_hit = False
        session.sheet_name = sheet_name
        sampled_load_ms = round((perf_counter() - sampled_load_started_at) * 1000, 3)
        rows_loaded = int(len(df))
        header_plan = df.attrs.get(HEADER_PLAN_ATTR) if isinstance(df.attrs.get(HEADER_PLAN_ATTR), dict) else None
        header_health = df.attrs.get(HEADER_HEALTH_ATTR) if isinstance(df.attrs.get(HEADER_HEALTH_ATTR), dict) else None

        log_event(
            logger,
            "spreadsheet_chat_started",
            request_id=request_id,
            file_id=file_id,
            conversation_id=session.conversation_id,
            sheet_index=resolved_sheet_index,
            mode=mode,
            rows_loaded=rows_loaded,
            sampled_cache_hit=sampled_cache_hit,
            multi_sheet_detected=sheet_routing_observability["multi_sheet_detected"],
            clarification_sheet_count=sheet_routing_observability["clarification_sheet_count"],
            sheet_switch_count=sheet_routing_observability["sheet_switch_count"],
            multi_sheet_failure_reason=sheet_routing_observability["multi_sheet_failure_reason"],
            multi_sheet_top_failure_reasons=sheet_routing_observability["multi_sheet_top_failure_reasons"],
            followup_action=normalized_followup_action,
            followup_action_applied=sheet_routing_observability["followup_action_applied"],
            followup_action_target_sheet_index=sheet_routing_observability["followup_action_target_sheet_index"],
            task_step_started_count=task_step_started_count,
            task_step_completed_count=task_step_completed_count,
        )

        meta_event = {
            "request_id": request_id,
            "file_id": file_id,
            "conversation_id": session.conversation_id,
            "meta": {
                "request_id": request_id,
                "sheet_index": sheet_index,
                "requested_sheet_name": _sheet_name_for_index(workbook_context, sheet_index),
                "sheet_override": sheet_override,
                "sheet_name": sheet_name,
                "resolved_sheet_index": resolved_sheet_index,
                "resolved_sheet_name": sheet_name,
                "workbook_sheet_count": len(workbook_context.sheets),
                "rows_loaded": rows_loaded,
                "cols_loaded": int(len(df.columns)),
                "conversation_id": session.conversation_id,
                "followup_turn_count": int(followup_context.get("turn_count") or 0) if followup_context else 0,
                "followup_context_reset": context_reset,
                "followup_action": normalized_followup_action or None,
                "followup_action_target_sheet_index": (
                    int(followup_action_target.get("sheet_index") or 0)
                    if isinstance(followup_action_target, dict)
                    else None
                ),
                "analysis_anchor_reused": analysis_anchor_reused,
                "analysis_anchor": followup_analysis_anchor,
                "sheet_routing": _sheet_routing_payload(workbook_context, routing_decision, locale=locale),
                "sheet_sequence": sheet_sequence,
                "task_steps": task_step_state.get("task_steps"),
                "current_step_id": task_step_state.get("current_step_id"),
                "header_plan": header_plan,
                "header_health": header_health,
                "sampled_cache_hit": sampled_cache_hit,
                "sampled_load_ms": sampled_load_ms,
                "observability": {
                    "sheet_routing": sheet_routing_observability,
                    "task_steps": task_step_transition_events,
                },
            },
        }
        yield _sse(meta_event)

        def _load_exact_source_df() -> tuple[object, str]:
            exact_cache_key = "analysis_full"
            exact_cache_token = _cache_token(path, sheet_index=resolved_sheet_index, scope=exact_cache_key)
            exact_cached = conversation_store.get_cached_dataframe(
                session,
                cache_key=exact_cache_key,
                cache_token=exact_cache_token,
            )
            if exact_cached is not None:
                return exact_cached
            full_df, full_sheet_name = load_full_dataframe(
                path,
                sheet_index=resolved_sheet_index,
                header_plan=header_plan,
            )
            conversation_store.set_cached_dataframe(
                session,
                cache_key=exact_cache_key,
                cache_token=exact_cache_token,
                dataframe=full_df,
                sheet_name=full_sheet_name,
            )
            return full_df, full_sheet_name

        result = analyze(
            df,
            chat_text=chat_text,
            requested_mode=mode,
            locale=locale,
            rows_loaded=rows_loaded,
            followup_context=planner_followup_context,
            source_path=path,
            source_sheet_index=resolved_sheet_index,
            exact_source_df_loader=_load_exact_source_df,
        )
        result.pipeline["sheet_routing"] = _sheet_routing_payload(workbook_context, routing_decision, locale=locale)
        result.pipeline["sheet_sequence"] = sheet_sequence
        result.pipeline["task_steps"] = task_step_state.get("task_steps")
        result.pipeline["current_step_id"] = task_step_state.get("current_step_id")
        result.pipeline["followup_action"] = normalized_followup_action or None
        result.pipeline["followup_action_target_sheet_index"] = (
            int(followup_action_target.get("sheet_index") or 0)
            if isinstance(followup_action_target, dict)
            else None
        )
        result.pipeline["analysis_anchor_reused"] = analysis_anchor_reused
        result.pipeline["analysis_anchor"] = followup_analysis_anchor
        step_comparison_payload = _build_step_comparison_payload(
            previous_step=previous_step_summary,
            current_sheet_index=resolved_sheet_index,
            current_sheet_name=sheet_name,
            current_answer_summary=str(result.analysis_text or result.answer or ""),
            current_intent=str(
                ((result.pipeline.get("planner") or {}) if isinstance(result.pipeline, dict) else {}).get("intent") or ""
            ),
            current_result_row_count=int(result.pipeline.get("result_row_count") or 0),
        )
        if isinstance(step_comparison_payload, dict):
            result.pipeline["step_comparison"] = step_comparison_payload
        result.pipeline["source_sheet_index"] = resolved_sheet_index
        result.pipeline.setdefault("source_sheet_name", sheet_name)
        result.pipeline.setdefault("observability", {})
        result.pipeline["observability"]["request_id"] = request_id
        result.pipeline["observability"]["request_total_ms"] = round((perf_counter() - request_started_at) * 1000, 3)
        result.pipeline["observability"]["task_step_started_count"] = task_step_started_count
        result.pipeline["observability"]["task_step_completed_count"] = task_step_completed_count
        result.pipeline["observability"]["task_step_events"] = task_step_transition_events
        result.pipeline["observability"].update(sheet_routing_observability)

        pipeline_event = {
            "request_id": request_id,
            "file_id": file_id,
            "conversation_id": session.conversation_id,
            "execution_disclosure": result.execution_disclosure.model_dump(),
            "data_scope": result.execution_disclosure.data_scope,
            "scope_text": result.execution_disclosure.scope_text,
            "scope_warning": result.execution_disclosure.scope_warning,
            "exact_used": result.execution_disclosure.exact_used,
            "pipeline": result.pipeline,
        }
        yield _sse(pipeline_event)

        answer_event = {
            "request_id": request_id,
            "file_id": file_id,
            "conversation_id": session.conversation_id,
            "mode": result.mode,
            "answer": result.answer,
            "analysis_text": result.analysis_text or result.answer,
            "execution_disclosure": result.execution_disclosure.model_dump(),
        }
        answer_segments = (
            (((result.pipeline.get("answer_generation") or {}) if isinstance(result.pipeline, dict) else {}).get("segments"))
            if isinstance(result.pipeline, dict)
            else None
        )
        if isinstance(answer_segments, dict):
            answer_event["answer_segments"] = answer_segments
        if result.chart_spec is not None:
            answer_event["chart_spec"] = result.chart_spec
        if result.chart_data is not None:
            answer_event["chart_data"] = result.chart_data

        conversation_store.append_turn(
            session,
            build_turn_summary(
                question=chat_text,
                requested_mode=mode,
                result_mode=result.mode,
                pipeline=result.pipeline,
                answer=result.answer,
                analysis_text=result.analysis_text,
                chart_spec=result.chart_spec,
                execution_disclosure=result.execution_disclosure.model_dump(),
            ),
        )

        log_event(
            logger,
            "spreadsheet_chat_completed",
            request_id=request_id,
            file_id=file_id,
            conversation_id=session.conversation_id,
            planner_provider=str(((result.pipeline.get("planner") or {}) if isinstance(result.pipeline, dict) else {}).get("provider") or ""),
            answer_provider=str(((result.pipeline.get("answer_generation") or {}) if isinstance(result.pipeline, dict) else {}).get("provider_used") or ""),
            exact_used=result.execution_disclosure.exact_used,
            total_ms=result.pipeline.get("observability", {}).get("total_ms"),
            multi_sheet_detected=sheet_routing_observability["multi_sheet_detected"],
            clarification_sheet_count=sheet_routing_observability["clarification_sheet_count"],
            sheet_switch_count=sheet_routing_observability["sheet_switch_count"],
            multi_sheet_failure_reason=sheet_routing_observability["multi_sheet_failure_reason"],
            multi_sheet_top_failure_reasons=sheet_routing_observability["multi_sheet_top_failure_reasons"],
            followup_action=normalized_followup_action,
            followup_action_applied=sheet_routing_observability["followup_action_applied"],
            followup_action_target_sheet_index=sheet_routing_observability["followup_action_target_sheet_index"],
            task_step_started_count=task_step_started_count,
            task_step_completed_count=task_step_completed_count,
        )
        yield _sse(answer_event)
        yield _sse({"request_id": request_id, "file_id": file_id, "conversation_id": session.conversation_id, "answer": "<|EOS|>"})
    except Exception as exc:
        current_task_steps = (
            [item for item in task_step_state.get("task_steps", []) if isinstance(item, dict)]
            if isinstance(task_step_state.get("task_steps"), list)
            else []
        )
        _emit_task_step_failed_event(
            request_id=request_id,
            file_id=file_id,
            conversation_id=active_conversation_id,
            followup_action=normalized_followup_action,
            current_steps=current_task_steps,
            current_step_id=str(task_step_state.get("current_step_id") or ""),
            error_type=type(exc).__name__,
        )
        logger.exception(
            json.dumps(
                {
                    "event": "spreadsheet_chat_failed",
                    "request_id": request_id,
                    "file_id": file_id,
                    "conversation_id": active_conversation_id,
                },
                ensure_ascii=False,
            )
        )
        message = t(locale, "internal_error", request_id=request_id)
        yield _sse(
            {
                "request_id": request_id,
                "file_id": file_id,
                "conversation_id": active_conversation_id,
                "mode": "text",
                "answer": message,
                "analysis_text": message,
            }
        )
        yield _sse({"request_id": request_id, "file_id": file_id, "conversation_id": active_conversation_id, "answer": "<|EOS|>"})
