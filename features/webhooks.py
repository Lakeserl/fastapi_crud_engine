
from __future__ import annotations
import asyncio
import hashlib
import hmac
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal

import httpx

logger = logging.getLogger("fastapi_smart_crud.webhooks")


@dataclass
class WebhookEndpoint:
    url:    str
    events: list[str] = field(default_factory=lambda: ["*"])   # "*" = all events
    secret: str | None = None
    headers: dict[str, str] = field(default_factory=dict)

    def listens_to(self, event: str) -> bool:
        return "*" in self.events or event in self.events

    def sign(self, payload: str) -> str | None:
        if not self.secret:
            return None
        sig = hmac.new(
            self.secret.encode(), payload.encode(), hashlib.sha256
        ).hexdigest()
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
        """Dispatch event to all subscribed endpoints."""
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
            self._dispatch_celery(event, table, record_id, data)
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

    def _dispatch_celery(self, event: str, table: str, record_id: Any, data: dict) -> None:
        try:
            from celery import Celery  # type: ignore
            import os
            app = Celery(broker=os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0"))

            @app.task(bind=True, max_retries=self.max_retries, queue=self.celery_queue)
            def deliver_webhook(self_task, endpoint_url, endpoint_secret, payload_dict):
                import requests as req_sync
                body = json.dumps(payload_dict, default=str)
                hdrs = {"Content-Type": "application/json"}
                if endpoint_secret:
                    sig = hmac.new(endpoint_secret.encode(), body.encode(), hashlib.sha256).hexdigest()
                    hdrs["X-Webhook-Signature"] = f"sha256={sig}"
                try:
                    resp = req_sync.post(endpoint_url, data=body, headers=hdrs, timeout=self.timeout)
                    resp.raise_for_status()
                except Exception as exc:
                    raise self_task.retry(exc=exc, countdown=2 ** self_task.request.retries)

            payload_dict = {
                "event": event, "table": table,
                "record_id": str(record_id),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "data": data,
            }
            for ep in self.endpoints:
                if ep.listens_to(event):
                    deliver_webhook.delay(ep.url, ep.secret, payload_dict)

        except ImportError:
            logger.warning("Celery not installed. Falling back to async HTTP delivery.")
            asyncio.ensure_future(self.dispatch(event, table, record_id, data))