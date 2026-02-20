from __future__ import annotations

import asyncio
from typing import Any, AsyncGenerator

import pytest
from fastapi import Depends
from pydantic import BaseModel
from sqlalchemy import Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from starlette.requests import Request

from fastapi_crud_engine.core.exceptions import PermissionDeniedException
from fastapi_crud_engine.core.mixins import OptimisticLockMixin, SoftDeleteMixin
from fastapi_crud_engine.core.permissions import FieldPermissions
from fastapi_crud_engine.router import CRUDRouter


class Base(DeclarativeBase):
    pass


class RouterUser(SoftDeleteMixin, OptimisticLockMixin, Base):
    __tablename__ = "router_users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    secret: Mapped[str | None] = mapped_column(String(120), nullable=True)


class UserSchema(BaseModel):
    id: int | None = None
    name: str | None = None
    secret: str | None = None
    version_id: int | None = None

    model_config = {"from_attributes": True}


class UserCreate(BaseModel):
    name: str | None = None
    secret: str | None = None


class UserUpdate(BaseModel):
    name: str | None = None
    secret: str | None = None


class FakeCache:
    def __init__(self) -> None:
        self.values: dict[str, Any] = {}
        self.invalidated: list[str] = []

    async def get(self, key: str) -> Any | None:
        return self.values.get(key)

    async def set(self, key: str, value: Any) -> None:
        self.values[key] = value

    async def invalidate_model(self, table_name: str) -> None:
        self.invalidated.append(table_name)


class FakeWebhooks:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def dispatch(self, **kwargs: Any) -> None:
        self.calls.append(kwargs)


async def get_db() -> AsyncGenerator[object, None]:
    yield object()


async def actor_dep() -> str:
    return "dep-user"


async def current_user(user: str = Depends(actor_dep)) -> str:
    return user


def make_router(
    *,
    field_permissions: FieldPermissions | None = None,
    cache: FakeCache | None = None,
    webhooks: FakeWebhooks | None = None,
    disable: list[str] | None = None,
    get_current_user: Any = current_user,
) -> CRUDRouter:
    return CRUDRouter(
        model=RouterUser,
        schema=UserSchema,
        create_schema=UserCreate,
        update_schema=UserUpdate,
        db=get_db,
        prefix="/users",
        soft_delete=True,
        field_permissions=field_permissions,
        cache=cache,
        webhooks=webhooks,
        disable=disable or ["import"],
        get_current_user=get_current_user,
    )


def _request(headers: dict[str, str] | None = None, client_host: str = "127.0.0.1") -> Request:
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/users",
        "headers": [(k.lower().encode(), v.encode()) for k, v in (headers or {}).items()],
        "client": (client_host, 1000),
        "query_string": b"",
    }
    return Request(scope)


def test_router_callable_depends_detection() -> None:
    router = make_router()

    assert router._use_user_dependency is True
    assert router.pk_type is int
    assert router.pk_path == "/{pk}"


@pytest.mark.asyncio
async def test_router_get_user_str_supports_direct_callable() -> None:
    def direct_user(_: Any) -> str:
        return "sync-user"

    router = make_router(get_current_user=direct_user)
    actor = await router._get_user_str(_request())

    assert actor == "sync-user"


@pytest.mark.asyncio
async def test_router_get_user_str_handles_exceptions() -> None:
    def broken_user(_: Any) -> str:
        raise RuntimeError("boom")

    router = make_router(get_current_user=broken_user)
    actor = await router._get_user_str(_request())

    assert actor is None


def test_router_filter_write_data_applies_permissions() -> None:
    perms = FieldPermissions(read={"user": ["id", "name"]}, write={"user": ["name"]})
    router = make_router(field_permissions=perms)

    assert router._filter_write_data({"name": "alice"}, "user") == {"name": "alice"}
    with pytest.raises(PermissionDeniedException):
        router._filter_write_data({"name": "alice", "secret": "x"}, "user")


def test_router_filter_obj_and_list_for_role() -> None:
    perms = FieldPermissions(read={"user": ["id", "name"]}, write={"user": ["name"]})
    router = make_router(field_permissions=perms)

    obj = RouterUser(id=1, name="alice", secret="x", version_id=1)
    single = router._filter_obj_for_role(obj, "user")
    many = router._filter_list_for_role([obj], "user")

    assert single == {"id": 1, "name": "alice"}
    assert many == [{"id": 1, "name": "alice"}]


def test_router_get_request_meta_prefers_forwarded_for() -> None:
    router = make_router()

    ip, ua = router._get_request_meta(
        _request(
            headers={"X-Forwarded-For": "203.0.113.55,10.0.0.1", "User-Agent": "pytest-agent"},
            client_host="127.0.0.1",
        )
    )

    assert ip == "203.0.113.55"
    assert ua == "pytest-agent"


@pytest.mark.asyncio
async def test_router_cache_helpers_delegate_to_cache() -> None:
    cache = FakeCache()
    router = make_router(cache=cache)

    assert await router._cache_get("k1") is None
    await router._cache_set("k1", {"v": 1})
    assert await router._cache_get("k1") == {"v": 1}

    await router._cache_invalidate()
    assert cache.invalidated == ["router_users"]


@pytest.mark.asyncio
async def test_router_emit_dispatches_webhook_task() -> None:
    hooks = FakeWebhooks()
    router = make_router(webhooks=hooks)
    obj = RouterUser(id=1, name="alice", secret=None, version_id=1)

    await router._emit("created", 1, obj)
    await asyncio.sleep(0)

    assert len(hooks.calls) == 1
    assert hooks.calls[0]["event"] == "routeruser.created"
    assert hooks.calls[0]["table"] == "router_users"
    assert hooks.calls[0]["record_id"] == 1


@pytest.mark.asyncio
async def test_router_emit_ignores_missing_running_loop(monkeypatch: pytest.MonkeyPatch) -> None:
    hooks = FakeWebhooks()
    router = make_router(webhooks=hooks)

    def raise_runtime_error(coroutine):
        coroutine.close()
        raise RuntimeError("no running loop")

    monkeypatch.setattr("fastapi_crud_engine.router.asyncio.create_task", raise_runtime_error)

    await router._emit("created", 1, RouterUser(id=1, name="alice", version_id=1))

    assert hooks.calls == []


def test_router_soft_delete_requires_soft_delete_mixin() -> None:
    class PlainModel(Base):
        __tablename__ = "plain_model"

        id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
        name: Mapped[str] = mapped_column(String(120), nullable=False)

    class PlainSchema(BaseModel):
        id: int | None = None
        name: str | None = None

        model_config = {"from_attributes": True}

    with pytest.raises(RuntimeError, match="must inherit SoftDeleteMixin"):
        CRUDRouter(
            model=PlainModel,
            schema=PlainSchema,
            db=get_db,
            prefix="/plain",
            soft_delete=True,
        )


def test_router_disable_routes_removes_endpoints() -> None:
    router = make_router(disable=["import", "bulk", "export", "restore", "deleted"])
    paths = {route.path for route in router.routes}

    assert "/users/bulk" not in paths
    assert "/users/export" not in paths
    assert "/users/deleted" not in paths
    assert "/users/{pk}/restore" not in paths
