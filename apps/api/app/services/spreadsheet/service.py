from __future__ import annotations

from collections import Counter
import json
import logging
from pathlib import Path
from threading import Lock
from time import perf_counter
from typing import AsyncGenerator

from app.observability import log_event, new_request_id
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
    request_id: str | None = None,
) -> AsyncGenerator[str, None]:
    request_id = str(request_id or new_request_id())
    request_started_at = perf_counter()
    settings = get_settings()
    try:
        session, context_reset = conversation_store.ensure_session(
            conversation_id=conversation_id,
            file_id=file_id,
            sheet_index=sheet_index,
            locale=locale,
        )
        followup_context = conversation_store.build_followup_context(
            session,
            chat_text=chat_text,
            clarification_resolution=clarification_resolution,
        )
        workbook_context = read_workbook_context(
            path,
            file_id=file_id,
            active_sheet_index=sheet_index,
            preview_limit=min(8, int(settings.max_analysis_rows)),
        )
        routing_decision = route_sheet(
            workbook_context,
            chat_text=chat_text,
            requested_sheet_index=sheet_index,
            requested_sheet_override=sheet_override,
            followup_context=followup_context,
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
        )
        planner_followup_context = dict(followup_context) if isinstance(followup_context, dict) else followup_context
        if isinstance(planner_followup_context, dict):
            planner_followup_context["current_sheet_index"] = resolved_sheet_index
            planner_followup_context["current_sheet_name"] = resolved_sheet_name
            planner_followup_context["sheet_switched_from_previous"] = bool(sheet_sequence.get("switched_from_previous"))
            planner_followup_context["sheet_switch_reason"] = str(sheet_sequence.get("last_sheet_switch_reason") or "")
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
                    "sheet_routing": _sheet_routing_payload(workbook_context, routing_decision, locale=locale),
                    "sheet_sequence": sheet_sequence,
                    "observability": {"sheet_routing": sheet_routing_observability},
                },
            }
            yield _sse(meta_event)

            pipeline = {
                "status": "clarification",
                "clarification_stage": "sheet_routing",
                "sheet_routing": _sheet_routing_payload(workbook_context, routing_decision, locale=locale),
                "sheet_sequence": sheet_sequence,
                "clarification": routing_decision.clarification.model_dump(),
                "workbook_sheets": [sheet.model_dump() for sheet in workbook_context.sheets],
                "observability": {
                    "request_id": request_id,
                    "request_total_ms": round((perf_counter() - request_started_at) * 1000, 3),
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
                "sheet_routing": _sheet_routing_payload(workbook_context, routing_decision, locale=locale),
                "sheet_sequence": sheet_sequence,
                "header_plan": header_plan,
                "header_health": header_health,
                "sampled_cache_hit": sampled_cache_hit,
                "sampled_load_ms": sampled_load_ms,
                "observability": {"sheet_routing": sheet_routing_observability},
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
        result.pipeline["source_sheet_index"] = resolved_sheet_index
        result.pipeline.setdefault("source_sheet_name", sheet_name)
        result.pipeline.setdefault("observability", {})
        result.pipeline["observability"]["request_id"] = request_id
        result.pipeline["observability"]["request_total_ms"] = round((perf_counter() - request_started_at) * 1000, 3)
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
        )
        yield _sse(answer_event)
        yield _sse({"request_id": request_id, "file_id": file_id, "conversation_id": session.conversation_id, "answer": "<|EOS|>"})
    except Exception:
        logger.exception(
            json.dumps(
                {
                    "event": "spreadsheet_chat_failed",
                    "request_id": request_id,
                    "file_id": file_id,
                    "conversation_id": conversation_id or "",
                },
                ensure_ascii=False,
            )
        )
        message = t(locale, "internal_error", request_id=request_id)
        yield _sse(
            {
                "request_id": request_id,
                "file_id": file_id,
                "conversation_id": conversation_id,
                "mode": "text",
                "answer": message,
                "analysis_text": message,
            }
        )
        yield _sse({"request_id": request_id, "file_id": file_id, "conversation_id": conversation_id, "answer": "<|EOS|>"})
