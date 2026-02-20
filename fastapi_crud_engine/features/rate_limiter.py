from __future__ import annotations
import time
import os
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Callable, Any
from fastapi import Request
from ..core.exceptions import RateLimitException

def _key_by_ip(request: Request) -> str:
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def _key_by_api_key(request: Request) -> str:
    return request.headers.get("X-API-Key") or _key_by_ip(request)

class _InMemoryBackend:
    def __init__(self):
        self._windows: dict[str, list[float]] = defaultdict(list)

    def is_allowed(self, key: str, limit: int, window: int) -> tuple[bool, int]:
        now = time.time()
        cutoff = now - window
        timestamps = self._windows[key]

        # Remove expired
        self._windows[key] = [t for t in timestamps if t > cutoff]
        count = len(self._windows[key])

        if count >= limit:
            oldest   = self._windows[key][0]
            retry    = int(oldest + window - now) + 1
            return False, retry

        self._windows[key].append(now)
        return True, 0

class _RedisBackend:

    def __init__(self, redis_url: str):
        import redis.asyncio as aioredis 
        self._redis = aioredis.from_url(redis_url, decode_responses=True)

    def is_allowed_sync(self, key: str, limit: int, window: int) -> tuple[bool, int]:
        raise NotImplementedError("Use is_allowed_async")

    async def is_allowed(self, key: str, limit: int, window: int) -> tuple[bool, int]:
        import redis.asyncio as aioredis
        now    = time.time()
        cutoff = now - window
        rkey   = f"ratelimit:{key}"

        pipe = self._redis.pipeline()
        await pipe.zremrangebyscore(rkey, 0, cutoff)
        await pipe.zcard(rkey)
        await pipe.zadd(rkey, {str(now): now})
        await pipe.expire(rkey, window + 1)
        results = await pipe.execute()

        count = results[1]
        if count > limit:
            await self._redis.zrem(rkey, str(now))
            oldest_score = await self._redis.zrange(rkey, 0, 0, withscores=True)
            if oldest_score:
                retry = int(oldest_score[0][1] + window - now) + 1
            else:
                retry = window
            return False, retry

        return True, 0

@dataclass
class RateLimiter:
    requests:  int = 100
    window:    int = 60
    key_func:  Callable[[Request], str] = field(default_factory=lambda: _key_by_ip)
    redis_url: str | None = None

    def __post_init__(self):
        self._backend: _InMemoryBackend | _RedisBackend | None = None

    def _get_backend(self) -> _InMemoryBackend | _RedisBackend:
        if self._backend is not None:
            return self._backend

        url = self.redis_url or os.getenv("REDIS_URL")
        if url:
            try:
                self._backend = _RedisBackend(url)
                return self._backend
            except ImportError:
                pass 

        self._backend = _InMemoryBackend()
        return self._backend

    async def check(self, request: Request) -> None:
        key     = self.key_func(request)
        backend = self._get_backend()

        if isinstance(backend, _RedisBackend):
            allowed, retry = await backend.is_allowed(key, self.requests, self.window)
        else:
            allowed, retry = backend.is_allowed(key, self.requests, self.window)

        if not allowed:
            raise RateLimitException(retry_after=retry)
