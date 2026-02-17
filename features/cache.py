from __future__ import annotations
import hashlib
import json
import os
import time
from dataclasses import dataclass, field
from typing import Any, Literal

class _TTLEntry:
    __slots__ = ("value", "expires_at")

    def __init__(self, value: Any, ttl: int):
        self.value      = value
        self.expires_at = time.monotonic() + ttl


class _InMemoryCache:
    def __init__(self, max_size: int = 10_000):
        self._store: dict[str, _TTLEntry] = {}
        self._max   = max_size

    def get(self, key: str) -> Any | None:
        entry = self._store.get(key)
        if entry is None:
            return None
        if time.monotonic() > entry.expires_at:
            del self._store[key]
            return None
        return entry.value

    def set(self, key: str, value: Any, ttl: int) -> None:
        if len(self._store) >= self._max:
            to_remove = list(self._store.keys())[: self._max // 10]
            for k in to_remove:
                self._store.pop(k, None)
        self._store[key] = _TTLEntry(value, ttl)

    def delete(self, key: str) -> None:
        self._store.pop(key, None)

    def delete_pattern(self, pattern: str) -> None:
        keys = [k for k in self._store if k.startswith(pattern.rstrip("*"))]
        for k in keys:
            del self._store[k]

    def clear(self) -> None:
        self._store.clear()

class _RedisCache:
    def __init__(self, redis_url: str):
        import redis.asyncio as aioredis 
        self._r = aioredis.from_url(redis_url, decode_responses=True)

    async def get(self, key: str) -> Any | None:
        val = await self._r.get(key)
        if val is None:
            return None
        return json.loads(val)

    async def set(self, key: str, value: Any, ttl: int) -> None:
        await self._r.setex(key, ttl, json.dumps(value, default=str))

    async def delete(self, key: str) -> None:
        await self._r.delete(key)

    async def delete_pattern(self, pattern: str) -> None:
        keys = await self._r.keys(pattern)
        if keys:
            await self._r.delete(*keys)

    async def clear(self) -> None:
        await self._r.flushdb()

@dataclass
class Cache:
    ttl:       int = 60
    backend:   Literal["auto", "memory", "redis"] = "auto"
    redis_url: str | None = None
    max_size:  int = 10_000

    def __post_init__(self):
        self._impl: _InMemoryCache | _RedisCache | None = None

    def _get_impl(self):
        if self._impl is not None:
            return self._impl

        if self.backend == "redis":
            url = self.redis_url or os.getenv("REDIS_URL", "redis://localhost:6379")
            self._impl = _RedisCache(url)
        elif self.backend == "memory":
            self._impl = _InMemoryCache(self.max_size)
        else:  
            url = self.redis_url or os.getenv("REDIS_URL")
            if url:
                try:
                    self._impl = _RedisCache(url)
                    return self._impl
                except ImportError:
                    pass
            self._impl = _InMemoryCache(self.max_size)

        return self._impl

    @staticmethod
    def make_key(prefix: str, **kwargs) -> str:
        payload = json.dumps(kwargs, sort_keys=True, default=str)
        digest  = hashlib.md5(payload.encode()).hexdigest()[:12]
        return f"{prefix}:{digest}"

    async def get(self, key: str) -> Any | None:
        impl = self._get_impl()
        if isinstance(impl, _RedisCache):
            return await impl.get(key)
        return impl.get(key)

    async def set(self, key: str, value: Any) -> None:
        impl = self._get_impl()
        if isinstance(impl, _RedisCache):
            await impl.set(key, value, self.ttl)
        else:
            impl.set(key, value, self.ttl)

    async def delete(self, key: str) -> None:
        impl = self._get_impl()
        if isinstance(impl, _RedisCache):
            await impl.delete(key)
        else:
            impl.delete(key)

    async def invalidate_model(self, table_name: str) -> None:
        impl = self._get_impl()
        pattern = f"crud:{table_name}:*"
        if isinstance(impl, _RedisCache):
            await impl.delete_pattern(pattern)
        else:
            impl.delete_pattern(pattern)