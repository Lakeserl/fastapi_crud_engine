from __future__ import annotations
from datetime import datetime, timezone
from sqlalchemy import DateTime, Integer, String, func
from sqlalchemy.orm import Mapped, declared_attr, declarative_mixin, mapped_column

@declarative_mixin
class TimestampMixin:
    @declared_attr
    def created_at(cls) -> Mapped[datetime]:
        return mapped_column(
            DateTime(timezone=True),
            server_default=func.now(),
            nullable=False,
        )

    @declared_attr
    def updated_at(cls) -> Mapped[datetime]:
        return mapped_column(
            DateTime(timezone=True),
            server_default=func.now(),
            onupdate=func.now(),
            nullable=False,
        )

@declarative_mixin
class SoftDeleteMixin:
    @declared_attr
    def deleted_at(cls) -> Mapped[datetime | None]:
        return mapped_column(
            DateTime(timezone=True),
            nullable=True,
            default=None,
            index=True,
        )

    @property
    def is_deleted(self) -> bool:
        return self.deleted_at is not None

    def soft_delete(self) -> None:
        self.deleted_at = datetime.now(timezone.utc)

    def restore(self) -> None:
        self.deleted_at = None

@declarative_mixin
class OptimisticLockMixin:
    @declared_attr
    def version_id(cls) -> Mapped[int]:
        return mapped_column(Integer, nullable=False, default=1)

@declarative_mixin
class AuditMixin(TimestampMixin):
    @declared_attr
    def created_by(cls) -> Mapped[str | None]:
        return mapped_column(String(255), nullable=True)

    @declared_attr
    def updated_by(cls) -> Mapped[str | None]:
        return mapped_column(String(255), nullable=True)
