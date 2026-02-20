from __future__ import annotations

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from fastapi_crud_engine.core.exceptions import PermissionDeniedException, RateLimitException
from fastapi_crud_engine.core.handlers import register_exception_handlers


@pytest.mark.asyncio
async def test_registered_handler_returns_structured_error_payload() -> None:
    app = FastAPI()
    register_exception_handlers(app)

    @app.get("/forbidden")
    async def forbidden() -> None:
        raise PermissionDeniedException("nope")

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/forbidden")

    assert resp.status_code == 403
    assert resp.json() == {"message": "nope"}


@pytest.mark.asyncio
async def test_registered_handler_sets_retry_after_header() -> None:
    app = FastAPI()
    register_exception_handlers(app)

    @app.get("/limited")
    async def limited() -> None:
        raise RateLimitException(retry_after=8)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/limited")

    assert resp.status_code == 429
    assert resp.headers["Retry-After"] == "8"
    assert resp.json() == {"message": "Rate limit exceeded. Retry after 8s."}
