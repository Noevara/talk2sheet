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
