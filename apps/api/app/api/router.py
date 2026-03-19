from __future__ import annotations

from fastapi import APIRouter

from app.api.routes.files import router as files_router
from app.api.routes.health import router as health_router
from app.api.routes.spreadsheet import router as spreadsheet_router


api_router = APIRouter()
api_router.include_router(health_router, tags=["system"])
api_router.include_router(files_router, tags=["files"])
api_router.include_router(spreadsheet_router, tags=["spreadsheet"])

