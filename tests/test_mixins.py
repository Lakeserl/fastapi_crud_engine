from __future__ import annotations

from datetime import timezone

from sqlalchemy import Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from fastapi_crud_engine.core.mixins import (
    AuditMixin,
    OptimisticLockMixin,
    SoftDeleteMixin,
)


class Base(DeclarativeBase):
    pass


class MixedModel(AuditMixin, SoftDeleteMixin, OptimisticLockMixin, Base):
    __tablename__ = "mixed_models"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(120), nullable=False)


def test_soft_delete_mixin_methods_work() -> None:
    obj = MixedModel(name="soft", created_by="tester")
    assert obj.deleted_at is None
    assert obj.is_deleted is False

    obj.soft_delete()
    assert obj.deleted_at is not None
    assert obj.deleted_at.tzinfo is timezone.utc
    assert obj.is_deleted is True

    obj.restore()
    assert obj.deleted_at is None
    assert obj.is_deleted is False


def test_mixin_columns_are_declared_with_expected_defaults() -> None:
    table = MixedModel.__table__

    assert table.c.version_id.default is not None
    assert table.c.version_id.default.arg == 1
    assert table.c.version_id.nullable is False

    assert table.c.created_at.server_default is not None
    assert table.c.updated_at.server_default is not None

    assert table.c.created_by.nullable is True
    assert table.c.updated_by.nullable is True
    assert table.c.deleted_at.nullable is True
