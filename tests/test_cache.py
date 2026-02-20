from __future__ import annotations

from typing import Any

import pytest

from fastapi_crud_engine.features.cache import Cache, _InMemoryCache


def test_inmemory_cache_set_get_delete_and_eviction(monkeypatch: pytest.MonkeyPatch) -> None:
    now = 100.0

    def fake_monotonic() -> float:
        return now

    monkeypatch.setattr("fastapi_crud_engine.features.cache.time.monotonic", fake_monotonic)

    cache = _InMemoryCache(max_size=2)
    cache.set("k1", "v1", ttl=10)
    cache.set("k2", "v2", ttl=10)
    assert cache.get("k1") == "v1"

    cache.set("k3", "v3", ttl=10)
    assert cache.get("k1") == "v1"
    assert cache.get("k2") is None

    cache.delete("k2")
    assert cache.get("k2") is None


def test_inmemory_cache_ttl_expires(monkeypatch: pytest.MonkeyPatch) -> None:
    current = 100.0

    def fake_monotonic() -> float:
        return current

    monkeypatch.setattr("fastapi_crud_engine.features.cache.time.monotonic", fake_monotonic)

    cache = _InMemoryCache()
    cache.set("k", "v", ttl=5)
    assert cache.get("k") == "v"

    current = 106.0
    assert cache.get("k") is None


@pytest.mark.asyncio
async def test_cache_memory_backend_crud_and_invalidate() -> None:
    cache = Cache(ttl=60, backend="memory", max_size=100)

    await cache.set("crud:users:list:1", {"items": [1]})
    await cache.set("crud:users:get:1", {"id": 1})
    await cache.set("other:key", 1)

    assert await cache.get("crud:users:get:1") == {"id": 1}

    await cache.invalidate_model("users")

    assert await cache.get("crud:users:list:1") is None
    assert await cache.get("crud:users:get:1") is None
    assert await cache.get("other:key") == 1


@pytest.mark.asyncio
async def test_cache_make_key_is_deterministic() -> None:
    key1 = Cache.make_key("crud:users:list", page=1, size=20, role="user")
    key2 = Cache.make_key("crud:users:list", role="user", size=20, page=1)
    key3 = Cache.make_key("crud:users:list", page=2, size=20, role="user")

    assert key1 == key2
    assert key1 != key3


def test_cache_auto_backend_falls_back_to_memory(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("REDIS_URL", raising=False)
    cache = Cache(backend="auto")

    impl = cache._get_impl()

    assert isinstance(impl, _InMemoryCache)
