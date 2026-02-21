from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest
from sqlalchemy import ForeignKey, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from fastapi_crud_engine.core.exceptions import (
    BulkOperationException,
    NotFoundException,
    OptimisticLockException,
)
from fastapi_crud_engine.core.mixins import OptimisticLockMixin, SoftDeleteMixin
from fastapi_crud_engine.core.pagination import PageParams
from fastapi_crud_engine.repository import CRUDRepository


class Base(DeclarativeBase):
    pass


class RepoUser(SoftDeleteMixin, OptimisticLockMixin, Base):
    __tablename__ = "repo_users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    email: Mapped[str | None] = mapped_column(String(120), nullable=True)
    profiles: Mapped[list["RepoProfile"]] = relationship(
        back_populates="user",
        cascade="all, delete-orphan",
    )


class RepoProfile(Base):
    __tablename__ = "repo_profiles"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("repo_users.id"), nullable=False)
    bio: Mapped[str] = mapped_column(String(255), nullable=False)
    user: Mapped["RepoUser"] = relationship(back_populates="profiles")


class HardUser(Base):
    __tablename__ = "hard_users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(120), nullable=False)


class _FakeScalarCollection:
    def __init__(self, items: list[Any]):
        self._items = items

    def all(self) -> list[Any]:
        return self._items


class _FakeResult:
    def __init__(
        self,
        *,
        scalar_one_or_none: Any = None,
        scalar_one: Any = None,
        scalar: Any = None,
        items: list[Any] | None = None,
    ) -> None:
        self._scalar_one_or_none = scalar_one_or_none
        self._scalar_one = scalar_one
        self._scalar = scalar
        self._items = items or []

    def scalar_one_or_none(self) -> Any:
        return self._scalar_one_or_none

    def scalar_one(self) -> Any:
        return self._scalar_one

    def scalar(self) -> Any:
        return self._scalar

    def scalars(self) -> _FakeScalarCollection:
        return _FakeScalarCollection(self._items)


class _FakeNestedTransaction:
    async def __aenter__(self) -> "_FakeNestedTransaction":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        return False


@dataclass
class FakeDB:
    execute_results: list[_FakeResult] = field(default_factory=list)
    added: list[Any] = field(default_factory=list)
    deleted: list[Any] = field(default_factory=list)
    commits: int = 0
    rollbacks: int = 0
    refreshes: int = 0
    flush_calls: int = 0

    async def execute(self, _query: Any) -> _FakeResult:
        if not self.execute_results:
            return _FakeResult()
        return self.execute_results.pop(0)

    def begin_nested(self) -> _FakeNestedTransaction:
        return _FakeNestedTransaction()

    def add(self, obj: Any) -> None:
        if getattr(obj, "id", None) is None:
            obj.id = len(self.added) + 1
        self.added.append(obj)

    async def flush(self) -> None:
        self.flush_calls += 1
        if self.added and getattr(self.added[-1], "name", "__ok__") is None:
            raise ValueError("name cannot be None")

    async def commit(self) -> None:
        self.commits += 1

    async def rollback(self) -> None:
        self.rollbacks += 1

    async def refresh(self, _obj: Any) -> None:
        self.refreshes += 1

    async def delete(self, obj: Any) -> None:
        self.deleted.append(obj)


@pytest.mark.asyncio
async def test_repository_get_not_found_raises() -> None:
    db = FakeDB(execute_results=[_FakeResult(scalar_one_or_none=None)])
    repo = CRUDRepository(RepoUser, soft_delete=True)

    with pytest.raises(NotFoundException):
        await repo.get(db, 999)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_repository_list_builds_page_response() -> None:
    users = [RepoUser(id=1, name="a"), RepoUser(id=2, name="b")]
    db = FakeDB(
        execute_results=[
            _FakeResult(scalar_one=2),
            _FakeResult(items=users),
        ]
    )
    repo = CRUDRepository(RepoUser, soft_delete=True)

    page = await repo.list(db, params=PageParams(page=1, size=20))  # type: ignore[arg-type]

    assert page.total == 2
    assert page.page == 1
    assert page.items == users


@pytest.mark.asyncio
async def test_repository_bulk_create_rolls_back_on_error() -> None:
    db = FakeDB()
    repo = CRUDRepository(RepoUser)

    with pytest.raises(BulkOperationException) as exc:
        await repo.bulk_create(
            db,  # type: ignore[arg-type]
            [{"name": "ok-user"}, {"name": None}],
            changed_by="tester",
        )

    assert len(exc.value.errors) == 1
    assert db.rollbacks == 1
    assert db.commits == 0


@pytest.mark.asyncio
async def test_repository_update_checks_optimistic_version() -> None:
    db = FakeDB()
    repo = CRUDRepository(RepoUser)
    obj = RepoUser(id=10, name="before", version_id=2)

    async def fake_get(_db: Any, _pk: Any) -> RepoUser:
        return obj

    repo.get = fake_get  # type: ignore[method-assign]

    with pytest.raises(OptimisticLockException):
        await repo.update(db, 10, {"name": "after"}, expected_version=1)  # type: ignore[arg-type]

    updated = await repo.update(db, 10, {"name": "after"}, expected_version=2)  # type: ignore[arg-type]
    assert updated.name == "after"
    assert db.commits == 1


@pytest.mark.asyncio
async def test_repository_delete_soft_and_hard_paths() -> None:
    soft_db = FakeDB()
    soft_repo = CRUDRepository(RepoUser, soft_delete=True)
    soft_obj = RepoUser(id=1, name="soft")

    async def fake_soft_get(_db: Any, _pk: Any) -> RepoUser:
        return soft_obj

    soft_repo.get = fake_soft_get  # type: ignore[method-assign]
    deleted = await soft_repo.delete(soft_db, 1)  # type: ignore[arg-type]

    assert deleted.deleted_at is not None
    assert soft_db.deleted == []
    assert soft_db.refreshes == 1

    hard_db = FakeDB()
    hard_repo = CRUDRepository(HardUser)
    hard_obj = HardUser(id=2, name="hard")

    async def fake_hard_get(_db: Any, _pk: Any) -> HardUser:
        return hard_obj

    hard_repo.get = fake_hard_get  # type: ignore[method-assign]
    await hard_repo.delete(hard_db, 2)  # type: ignore[arg-type]

    assert hard_db.deleted == [hard_obj]


@pytest.mark.asyncio
async def test_repository_restore_uses_get_deleted_and_commits() -> None:
    db = FakeDB()
    repo = CRUDRepository(RepoUser, soft_delete=True)
    obj = RepoUser(id=3, name="restore-me")
    obj.soft_delete()

    async def fake_get_deleted(_db: Any, _pk: Any) -> RepoUser:
        return obj

    repo.get_deleted = fake_get_deleted  # type: ignore[method-assign]
    restored = await repo.restore(db, 3)  # type: ignore[arg-type]

    assert restored.deleted_at is None
    assert db.commits == 1
    assert db.refreshes == 1


def test_repository_extract_nested_splits_relationship_payload() -> None:
    repo = CRUDRepository(RepoUser)

    clean, nested = repo._extract_nested(
        {
            "name": "nested-user",
            "email": "u@example.com",
            "profiles": [{"bio": "first"}],
        }
    )

    assert clean == {"name": "nested-user", "email": "u@example.com"}
    assert nested == {"profiles": [{"bio": "first"}]}
