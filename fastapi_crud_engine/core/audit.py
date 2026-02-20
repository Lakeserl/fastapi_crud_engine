from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import DateTime, Integer, String, Text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def build_audit_log_model(base: type[DeclarativeBase]) -> type[DeclarativeBase]:
    for mapper in base.registry.mappers:
        if mapper.class_.__name__ == "AuditLog":
            return mapper.class_

    class AuditLog(base):  
        __tablename__ = "audit_logs"

        id: Mapped[int] = mapped_column(
            Integer, primary_key=True, autoincrement=True
        )
        table_name: Mapped[str] = mapped_column(
            String(255), nullable=False, index=True
        )
        record_id: Mapped[str] = mapped_column(
            String(255), nullable=False, index=True
        )
        action: Mapped[str] = mapped_column(String(32), nullable=False)
        changed_by: Mapped[str | None] = mapped_column(
            String(255), nullable=True
        )
        old_values: Mapped[str | None] = mapped_column(Text, nullable=True)
        new_values: Mapped[str | None] = mapped_column(Text, nullable=True)
        ip_address: Mapped[str | None] = mapped_column(
            String(64), nullable=True
        )
        user_agent: Mapped[str | None] = mapped_column(
            String(512), nullable=True
        )
        timestamp: Mapped[datetime] = mapped_column(
            DateTime(timezone=True),
            default=lambda: datetime.now(timezone.utc),
            nullable=False,
            index=True,
        )

    return AuditLog


def _serialize(obj: Any) -> str | None:
    if obj is None:
        return None
    try:
        if isinstance(obj, dict):
            payload = obj
        else:
            mapper = getattr(obj, "__mapper__", None)
            if mapper is None:
                return json.dumps(obj, default=str)
            payload = {
                c.key: getattr(obj, c.key, None)
                for c in mapper.column_attrs
            }
        return json.dumps(payload, default=str)
    except Exception:
        return None


async def write_audit_log(
    db: AsyncSession,
    *,
    audit_log_model: type,
    table_name: str,
    record_id: Any,
    action: str,
    old_obj: Any = None,
    new_obj: Any = None,
    changed_by: str | None = None,
    ip_address: str | None = None,
    user_agent: str | None = None,
) -> None:
    log = audit_log_model(
        table_name=table_name,
        record_id=str(record_id),
        action=action.strip().upper(),
        changed_by=changed_by,
        old_values=_serialize(old_obj),
        new_values=_serialize(new_obj),
        ip_address=ip_address,
        user_agent=user_agent,
    )
    db.add(log)