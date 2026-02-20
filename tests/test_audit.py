from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import pytest
from sqlalchemy import Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from fastapi_crud_engine.core.audit import _serialize, build_audit_log_model, write_audit_log


class Base(DeclarativeBase):
    pass


class AuditSource(Base):
    __tablename__ = "audit_sources"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    title: Mapped[str] = mapped_column(String(255), nullable=False)


class _BadSerializable:
    def __str__(self) -> str:
        raise RuntimeError("cannot stringify")


@dataclass
class _FakeAsyncSession:
    added: list[Any]

    def __init__(self) -> None:
        self.added = []

    def add(self, obj: Any) -> None:
        self.added.append(obj)


class _FakeAuditLog:
    def __init__(self, **kwargs: Any) -> None:
        for key, value in kwargs.items():
            setattr(self, key, value)


def test_build_audit_log_model_is_idempotent() -> None:
    first = build_audit_log_model(Base)
    second = build_audit_log_model(Base)
    assert first is second
    assert first.__tablename__ == "audit_logs"


def test_serialize_handles_dict_and_mapper_object() -> None:
    src = AuditSource(id=1, title="hello")

    assert json.loads(_serialize({"x": 1}) or "{}") == {"x": 1}
    assert json.loads(_serialize(src) or "{}") == {"id": 1, "title": "hello"}


def test_serialize_failure_returns_none() -> None:
    assert _serialize({"bad": _BadSerializable()}) is None
    assert _serialize(_BadSerializable()) is None


@pytest.mark.asyncio
async def test_write_audit_log_adds_record_with_normalized_action() -> None:
    session = _FakeAsyncSession()

    await write_audit_log(
        session,  # type: ignore[arg-type]
        audit_log_model=_FakeAuditLog,
        table_name="audit_sources",
        record_id=10,
        action=" update ",
        old_obj={"title": "before"},
        new_obj=AuditSource(id=10, title="after"),
        changed_by="tester",
        ip_address="203.0.113.10",
        user_agent="pytest-agent/1.0",
    )

    assert len(session.added) == 1
    row = session.added[0]
    assert isinstance(row, _FakeAuditLog)
    assert row.action == "UPDATE"
    assert row.table_name == "audit_sources"
    assert row.record_id == "10"
    assert row.changed_by == "tester"
    assert row.ip_address == "203.0.113.10"
    assert row.user_agent == "pytest-agent/1.0"

    old_values = json.loads(row.old_values)
    new_values = json.loads(row.new_values)
    assert old_values["title"] == "before"
    assert new_values["title"] == "after"
