from __future__ import annotations

import json
import logging
from typing import Any
from uuid import uuid4

from fastapi import Request


REQUEST_ID_HEADER = "X-Request-ID"


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
