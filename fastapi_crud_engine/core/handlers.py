from __future__ import annotations

from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .exceptions import BulkOperationException, CRUDException, RateLimitException


def build_error_payload(exc: CRUDException) -> tuple[Any, dict[str, str]]:
    detail: Any = {"message": exc.detail}
    headers: dict[str, str] = {}

    if isinstance(exc, BulkOperationException):
        detail["errors"] = exc.errors
    if isinstance(exc, RateLimitException):
        headers["Retry-After"] = str(exc.retry_after)

    return detail, headers


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(CRUDException)
    async def _crud_exception_handler(_: Request, exc: CRUDException):
        detail, headers = build_error_payload(exc)
        return JSONResponse(
            status_code=exc.status_code,
            content={"detail": detail} if not isinstance(detail, dict) else detail,
            headers=headers,
        )
