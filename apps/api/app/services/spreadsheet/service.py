from __future__ import annotations

import json
import logging
from pathlib import Path
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


def _sheet_routing_payload(workbook_context: object, routing_decision: object) -> dict[str, object]:
    sheet_count = len(getattr(workbook_context, "sheets", []) or [])
    payload = dict(routing_decision.model_dump()) if hasattr(routing_decision, "model_dump") else dict(routing_decision)
    payload["workbook_sheet_count"] = sheet_count
    return payload


def _sheet_name_for_index(workbook_context: object, sheet_index: int) -> str:
    for sheet in getattr(workbook_context, "sheets", []) or []:
        if int(getattr(sheet, "sheet_index", 0) or 0) == int(sheet_index or 0):
            return str(getattr(sheet, "sheet_name", "") or "")
    return ""


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
                    "sheet_routing": _sheet_routing_payload(workbook_context, routing_decision),
                },
            }
            yield _sse(meta_event)

            pipeline = {
                "status": "clarification",
                "clarification_stage": "sheet_routing",
                "sheet_routing": _sheet_routing_payload(workbook_context, routing_decision),
                "clarification": routing_decision.clarification.model_dump(),
                "workbook_sheets": [sheet.model_dump() for sheet in workbook_context.sheets],
                "observability": {
                    "request_id": request_id,
                    "request_total_ms": round((perf_counter() - request_started_at) * 1000, 3),
                },
            }
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
                "sheet_routing": _sheet_routing_payload(workbook_context, routing_decision),
                "header_plan": header_plan,
                "header_health": header_health,
                "sampled_cache_hit": sampled_cache_hit,
                "sampled_load_ms": sampled_load_ms,
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
            followup_context=followup_context,
            source_path=path,
            source_sheet_index=resolved_sheet_index,
            exact_source_df_loader=_load_exact_source_df,
        )
        result.pipeline["sheet_routing"] = _sheet_routing_payload(workbook_context, routing_decision)
        result.pipeline["source_sheet_index"] = resolved_sheet_index
        result.pipeline.setdefault("source_sheet_name", sheet_name)
        result.pipeline.setdefault("observability", {})
        result.pipeline["observability"]["request_id"] = request_id
        result.pipeline["observability"]["request_total_ms"] = round((perf_counter() - request_started_at) * 1000, 3)

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
