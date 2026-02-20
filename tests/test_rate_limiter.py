from __future__ import annotations

from dataclasses import dataclass

import pytest
from starlette.requests import Request

from fastapi_crud_engine.core.exceptions import RateLimitException
from fastapi_crud_engine.features.rate_limiter import (
    RateLimiter,
    _InMemoryBackend,
    _key_by_api_key,
    _key_by_ip,
)


def _request(headers: dict[str, str] | None = None, client_host: str = "127.0.0.1") -> Request:
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/",
        "headers": [(k.lower().encode(), v.encode()) for k, v in (headers or {}).items()],
        "client": (client_host, 12345),
        "query_string": b"",
    }
    return Request(scope)


def test_key_helpers_prioritize_forwarded_and_api_key() -> None:
    req = _request({"X-Forwarded-For": "203.0.113.10, 10.0.0.2", "X-API-Key": "k-1"})

    assert _key_by_ip(req) == "203.0.113.10"
    assert _key_by_api_key(req) == "k-1"


@pytest.mark.asyncio
async def test_inmemory_backend_window_and_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    now = 1_000.0

    def fake_time() -> float:
        return now

    monkeypatch.setattr("fastapi_crud_engine.features.rate_limiter.time.time", fake_time)

    backend = _InMemoryBackend()
    assert backend.is_allowed("ip-1", limit=2, window=10) == (True, 0)
    assert backend.is_allowed("ip-1", limit=2, window=10) == (True, 0)

    allowed, retry = backend.is_allowed("ip-1", limit=2, window=10)
    assert allowed is False
    assert retry >= 1

    now = 1_020.0
    assert backend.is_allowed("ip-1", limit=2, window=10) == (True, 0)


@pytest.mark.asyncio
async def test_rate_limiter_check_raises_rate_limit_exception() -> None:
    limiter = RateLimiter(requests=1, window=60)
    req = _request()

    await limiter.check(req)
    with pytest.raises(RateLimitException) as exc:
        await limiter.check(req)

    assert exc.value.retry_after >= 1


@pytest.mark.asyncio
async def test_rate_limiter_auto_falls_back_to_memory_on_redis_import_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FailRedisBackend:
        def __init__(self, _url: str):
            raise ImportError("redis missing")

    monkeypatch.setattr("fastapi_crud_engine.features.rate_limiter._RedisBackend", _FailRedisBackend)
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379")

    limiter = RateLimiter(requests=2, window=60)
    backend = limiter._get_backend()

    assert isinstance(backend, _InMemoryBackend)


@pytest.mark.asyncio
async def test_rate_limiter_uses_custom_sync_backend_when_injected() -> None:
    @dataclass
    class _SyncBackend:
        called: int = 0

        def is_allowed(self, _key: str, _limit: int, _window: int):
            self.called += 1
            return False, 5

    limiter = RateLimiter(requests=5, window=60)
    limiter._backend = _SyncBackend()  # type: ignore[assignment]

    with pytest.raises(RateLimitException) as exc:
        await limiter.check(_request())

    assert exc.value.retry_after == 5
