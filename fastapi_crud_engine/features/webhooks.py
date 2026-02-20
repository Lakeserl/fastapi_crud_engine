from __future__ import annotations
import asyncio
import hashlib
import hmac
import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal

import httpx

logger = logging.getLogger("fastapi_smart_crud.webhooks")

_CELERY_APP: Any | None = None
_CELERY_DELIVER_TASK: Any | None = None


def _build_signature(secret: str, payload: str) -> str:
    return hmac.HMAC(secret.encode(), payload.encode(), hashlib.sha256).hexdigest()


def _get_celery_deliver_task() -> Any:
    global _CELERY_APP, _CELERY_DELIVER_TASK

    if _CELERY_DELIVER_TASK is not None:
        return _CELERY_DELIVER_TASK

    from celery import Celery  # type: ignore
    import requests as req_sync  # type: ignore

    _CELERY_APP = Celery(
        "fastapi_crud_engine.webhooks",
        broker=os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0"),
    )

    @_CELERY_APP.task(bind=True, name="fastapi_crud_engine.deliver_webhook")
    def deliver_webhook(  # type: ignore[misc]
        self_task,
        endpoint_url: str,
        endpoint_secret: str | None,
        endpoint_headers: dict[str, str],
        payload_dict: dict[str, Any],
        timeout: int,
        max_retries: int,
    ) -> None:
        body = json.dumps(payload_dict, default=str)
        headers = {
            "Content-Type": "application/json",
            "X-Webhook-Event": str(payload_dict.get("event", "")),
            **(endpoint_headers or {}),
        }
        if endpoint_secret:
            sig = _build_signature(endpoint_secret, body)
            headers["X-Webhook-Signature"] = f"sha256={sig}"
        try:
            resp = req_sync.post(
                endpoint_url,
                data=body,
                headers=headers,
                timeout=timeout,
            )
            resp.raise_for_status()
        except Exception as exc:
            retries = int(getattr(self_task.request, "retries", 0))
            if retries >= max_retries:
                raise
            raise self_task.retry(
                exc=exc,
                countdown=2 ** retries,
                max_retries=max_retries,
            )

    _CELERY_DELIVER_TASK = deliver_webhook
    return _CELERY_DELIVER_TASK


@dataclass
class WebhookEndpoint:
    url:    str
    events: list[str] = field(default_factory=lambda: ["*"])
    secret: str | None = None
    headers: dict[str, str] = field(default_factory=dict)

    def listens_to(self, event: str) -> bool:
        return "*" in self.events or event in self.events

    def sign(self, payload: str) -> str | None:
        if not self.secret:
            return None
        sig = _build_signature(self.secret, payload)
        return f"sha256={sig}"


@dataclass
class WebhookConfig:
    endpoints:   list[WebhookEndpoint] = field(default_factory=list)
    delivery:    Literal["http", "celery"] = "http"
    max_retries: int = 3
    timeout:     int = 10
    celery_queue: str = "webhooks"

    async def dispatch(
        self,
        event: str,
        table: str,
        record_id: Any,
        data: dict[str, Any],
    ) -> None:
        payload_dict = {
            "event":      event,
            "table":      table,
            "record_id":  str(record_id),
            "timestamp":  datetime.now(timezone.utc).isoformat(),
            "data":       data,
        }

        targets = [ep for ep in self.endpoints if ep.listens_to(event)]
        if not targets:
            return

        if self.delivery == "celery":
            self._dispatch_celery(targets, payload_dict)
        else:
            await asyncio.gather(
                *[self._deliver(ep, payload_dict) for ep in targets],
                return_exceptions=True,
            )

    async def _deliver(
        self,
        endpoint: WebhookEndpoint,
        payload: dict,
        attempt: int = 1,
    ) -> None:
        body    = json.dumps(payload, default=str)
        headers = {
            "Content-Type":    "application/json",
            "X-Webhook-Event": payload["event"],
            **endpoint.headers,
        }
        sig = endpoint.sign(body)
        if sig:
            headers["X-Webhook-Signature"] = sig

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                resp = await client.post(endpoint.url, content=body, headers=headers)
                resp.raise_for_status()
                logger.info("Webhook delivered: %s → %s (%d)", payload["event"], endpoint.url, resp.status_code)
        except Exception as exc:
            logger.warning(
                "Webhook attempt %d/%d failed: %s → %s: %s",
                attempt, self.max_retries, payload["event"], endpoint.url, exc,
            )
            if attempt < self.max_retries:
                backoff = 2 ** (attempt - 1)
                await asyncio.sleep(backoff)
                await self._deliver(endpoint, payload, attempt + 1)
            else:
                logger.error(
                    "Webhook permanently failed after %d attempts: %s",
                    self.max_retries, endpoint.url,
                )

    def _dispatch_celery(
        self,
        endpoints: list[WebhookEndpoint],
        payload_dict: dict[str, Any],
    ) -> None:
        try:
            deliver_webhook = _get_celery_deliver_task()
            for ep in endpoints:
                deliver_webhook.apply_async(
                    args=[
                        ep.url,
                        ep.secret,
                        ep.headers,
                        payload_dict,
                        self.timeout,
                        self.max_retries,
                    ],
                    queue=self.celery_queue,
                )
        except ImportError:
            logger.warning("Celery not installed. Falling back to async HTTP delivery.")
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                return
            for ep in endpoints:
                loop.create_task(self._deliver(ep, payload_dict))
