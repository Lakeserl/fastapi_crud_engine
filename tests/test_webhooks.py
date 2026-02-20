from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import Any
from unittest.mock import AsyncMock

import pytest

from fastapi_crud_engine.features.webhooks import (
    WebhookConfig,
    WebhookEndpoint,
    _build_signature,
)


def test_webhook_endpoint_listens_and_signs() -> None:
    ep = WebhookEndpoint(url="https://example.com", events=["user.created"], secret="s1")

    assert ep.listens_to("user.created") is True
    assert ep.listens_to("user.deleted") is False
    assert ep.sign("payload") == f"sha256={_build_signature('s1', 'payload')}"


@pytest.mark.asyncio
async def test_dispatch_calls_deliver_only_for_matching_endpoints() -> None:
    cfg = WebhookConfig(
        endpoints=[
            WebhookEndpoint(url="https://a.example", events=["user.created"]),
            WebhookEndpoint(url="https://b.example", events=["user.deleted"]),
            WebhookEndpoint(url="https://c.example", events=["*"]),
        ]
    )
    cfg._deliver = AsyncMock()  # type: ignore[method-assign]

    await cfg.dispatch("user.created", "users", 10, {"id": 10})

    assert cfg._deliver.await_count == 2


@pytest.mark.asyncio
async def test_deliver_builds_headers_and_posts_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []

    class _Resp:
        status_code = 200

        def raise_for_status(self) -> None:
            return None

    class _Client:
        def __init__(self, *, timeout: int):
            self.timeout = timeout

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url: str, content: str, headers: dict[str, str]):
            calls.append({"url": url, "content": content, "headers": headers, "timeout": self.timeout})
            return _Resp()

    monkeypatch.setattr("fastapi_crud_engine.features.webhooks.httpx.AsyncClient", _Client)

    cfg = WebhookConfig(max_retries=2, timeout=7)
    ep = WebhookEndpoint(
        url="https://example.com/webhook",
        events=["*"],
        secret="secret-1",
        headers={"X-Custom": "yes"},
    )
    payload = {"event": "user.created", "table": "users", "record_id": "1", "data": {"id": 1}}

    await cfg._deliver(ep, payload)

    assert len(calls) == 1
    assert calls[0]["url"] == "https://example.com/webhook"
    assert calls[0]["timeout"] == 7
    assert calls[0]["headers"]["Content-Type"] == "application/json"
    assert calls[0]["headers"]["X-Webhook-Event"] == "user.created"
    assert calls[0]["headers"]["X-Custom"] == "yes"
    assert calls[0]["headers"]["X-Webhook-Signature"].startswith("sha256=")
    assert json.loads(calls[0]["content"])["event"] == "user.created"


@pytest.mark.asyncio
async def test_deliver_retries_on_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    attempts = {"count": 0}

    class _Resp:
        status_code = 200

        def raise_for_status(self) -> None:
            return None

    class _Client:
        def __init__(self, *, timeout: int):
            self.timeout = timeout

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, _url: str, content: str, headers: dict[str, str]):
            attempts["count"] += 1
            if attempts["count"] == 1:
                raise RuntimeError("temporary failure")
            return _Resp()

    sleep_mock = AsyncMock()
    monkeypatch.setattr("fastapi_crud_engine.features.webhooks.httpx.AsyncClient", _Client)
    monkeypatch.setattr("fastapi_crud_engine.features.webhooks.asyncio.sleep", sleep_mock)

    cfg = WebhookConfig(max_retries=2, timeout=3)
    ep = WebhookEndpoint(url="https://example.com", events=["*"])

    await cfg._deliver(ep, {"event": "user.created"})

    assert attempts["count"] == 2
    sleep_mock.assert_awaited_once_with(1)


@pytest.mark.asyncio
async def test_dispatch_celery_mode_uses_apply_async(monkeypatch: pytest.MonkeyPatch) -> None:
    @dataclass
    class _Task:
        calls: list[dict[str, Any]]

        def apply_async(self, *, args: list[Any], queue: str) -> None:
            self.calls.append({"args": args, "queue": queue})

    task = _Task(calls=[])
    monkeypatch.setattr(
        "fastapi_crud_engine.features.webhooks._get_celery_deliver_task",
        lambda: task,
    )

    cfg = WebhookConfig(
        endpoints=[WebhookEndpoint(url="https://example.com", events=["*"])],
        delivery="celery",
        celery_queue="wh-q",
    )

    await cfg.dispatch("user.created", "users", 123, {"id": 123})

    assert len(task.calls) == 1
    assert task.calls[0]["queue"] == "wh-q"
    assert task.calls[0]["args"][0] == "https://example.com"
