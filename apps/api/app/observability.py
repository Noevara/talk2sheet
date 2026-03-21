from __future__ import annotations

import json
import logging
from typing import Any
from uuid import uuid4

from fastapi import Request


REQUEST_ID_HEADER = "X-Request-ID"
TASK_STEP_EVENTS = frozenset({"task_step_started", "task_step_completed", "task_step_failed"})


def new_request_id() -> str:
    return uuid4().hex


def get_request_id(request: Request) -> str:
    request_id = getattr(request.state, "request_id", "")
    return str(request_id or "")


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


def elapsed_ms(started_at: float) -> float:
    from time import perf_counter

    return round((perf_counter() - started_at) * 1000, 3)


def log_request_event(logger: logging.Logger, event: str, request_id: str, **fields: Any) -> None:
    log_event(logger, event, request_id=request_id, **fields)


def log_event(logger: logging.Logger, event: str, **fields: Any) -> None:
    logger.info(
        json.dumps(
            {
                "event": event,
                **fields,
            },
            ensure_ascii=False,
            default=str,
        )
    )


def log_task_step_event(
    logger: logging.Logger,
    *,
    event: str,
    request_id: str,
    file_id: str,
    conversation_id: str,
    step_id: str,
    step_status: str,
    sheet_index: int,
    sheet_name: str,
    followup_action: str = "",
    reason: str = "",
    error_type: str = "",
) -> None:
    normalized_event = str(event or "").strip()
    if normalized_event not in TASK_STEP_EVENTS:
        return
    normalized_step_id = str(step_id or "").strip()
    if not normalized_step_id:
        return
    payload = {
        "request_id": str(request_id or ""),
        "file_id": str(file_id or ""),
        "conversation_id": str(conversation_id or ""),
        "step_id": normalized_step_id,
        "step_status": str(step_status or "").strip(),
        "step_sheet_index": int(sheet_index or 0),
        "step_sheet_name": str(sheet_name or "").strip(),
    }
    if followup_action:
        payload["followup_action"] = str(followup_action).strip()
    if reason:
        payload["reason"] = str(reason).strip()
    if error_type:
        payload["error_type"] = str(error_type).strip()
    log_event(logger, normalized_event, **payload)
