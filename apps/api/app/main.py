from __future__ import annotations

import logging
from time import perf_counter

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.api.router import api_router
from app.config import get_settings
from app.observability import REQUEST_ID_HEADER, elapsed_ms, get_logger, get_request_id, log_request_event, new_request_id


settings = get_settings()
logger = get_logger(__name__)

app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins or ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def attach_request_id(request: Request, call_next):
    request_id = request.headers.get(REQUEST_ID_HEADER) or new_request_id()
    request.state.request_id = request_id
    started_at = perf_counter()
    log_request_event(
        logger,
        "http_request_started",
        request_id,
        method=request.method,
        path=request.url.path,
        query=str(request.url.query or ""),
    )
    try:
        response = await call_next(request)
    except Exception:
        log_request_event(
            logger,
            "http_request_failed_before_response",
            request_id,
            method=request.method,
            path=request.url.path,
            duration_ms=elapsed_ms(started_at),
        )
        raise
    response.headers[REQUEST_ID_HEADER] = request_id
    log_request_event(
        logger,
        "http_request_completed",
        request_id,
        method=request.method,
        path=request.url.path,
        status_code=response.status_code,
        duration_ms=elapsed_ms(started_at),
    )
    return response


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
    request_id = get_request_id(request) or new_request_id()
    log_request_event(
        logger,
        "http_exception",
        request_id,
        method=request.method,
        path=request.url.path,
        status_code=exc.status_code,
        detail=str(exc.detail),
    )
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "detail": exc.detail,
            "request_id": request_id,
        },
        headers={REQUEST_ID_HEADER: request_id},
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    request_id = get_request_id(request) or new_request_id()
    log_request_event(
        logger,
        "request_validation_failed",
        request_id,
        method=request.method,
        path=request.url.path,
        errors=exc.errors(),
    )
    return JSONResponse(
        status_code=422,
        content={
            "detail": "Request validation failed.",
            "request_id": request_id,
            "errors": exc.errors(),
        },
        headers={REQUEST_ID_HEADER: request_id},
    )


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    request_id = get_request_id(request) or new_request_id()
    logger.exception(
        "unhandled_request_exception",
        extra={
            "request_id": request_id,
            "method": request.method,
            "path": request.url.path,
        },
    )
    return JSONResponse(
        status_code=500,
        content={
            "detail": "Internal server error.",
            "request_id": request_id,
        },
        headers={REQUEST_ID_HEADER: request_id},
    )

app.include_router(api_router, prefix=settings.api_prefix)
