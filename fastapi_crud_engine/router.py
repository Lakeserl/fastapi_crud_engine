import asyncio
import inspect
import logging
from dataclasses import dataclass
from typing import Annotated, Any, Callable, Optional, get_args, get_origin
from uuid import UUID

from fastapi import APIRouter, Depends, File, HTTPException, Query, Request, Response, UploadFile
from fastapi.params import Depends as DependsParam
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute
from pydantic import BaseModel
from starlette.datastructures import QueryParams
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import DeclarativeBase

from .core.audit import build_audit_log_model
from .core.exceptions import (
    CRUDException,
    PermissionDeniedException,
)
from .core.filters import FilterSet
from .core.handlers import build_error_payload
from .core.mixins import SoftDeleteMixin
from .core.pagination import PageParams, PageResponse
from .core.permissions import FieldPermissions
from .features.cache import Cache
from .features.export_import import export_csv, export_excel, import_csv_or_excel
from .features.rate_limiter import RateLimiter
from .features.webhooks import WebhookConfig
from .repository import CRUDRepository

router_logger = logging.getLogger("fastapi_smart_crud.router")

HookCallable = Callable[..., Any]
HookSpec = HookCallable | list[HookCallable] | tuple[HookCallable, ...] | None


@dataclass
class CRUDHooks:
    before_create:  HookSpec = None
    after_create:   HookSpec = None
    before_update:  HookSpec = None
    after_update:   HookSpec = None
    before_delete:  HookSpec = None
    after_delete:   HookSpec = None
    before_restore: HookSpec = None
    after_restore:  HookSpec = None


async def _hook(hooks: HookSpec, *args) -> None:
    if hooks is None:
        return

    callables = hooks if isinstance(hooks, (list, tuple)) else [hooks]
    for fn in callables:
        if asyncio.iscoroutinefunction(fn):
            await fn(*args)
        else:
            fn(*args)


def _obj_to_dict(obj: Any) -> dict:
    try:
        return {c.key: getattr(obj, c.key) for c in obj.__mapper__.column_attrs}
    except Exception:
        return {}


class CRUDExceptionRoute(APIRoute):
    """Route class that maps library exceptions to HTTP responses."""
    def get_route_handler(self) -> Callable:
        original_handler = super().get_route_handler()

        async def custom_handler(request: Request) -> Response:
            try:
                return await original_handler(request)
            except CRUDException as exc:
                detail, headers = build_error_payload(exc)
                raise HTTPException(
                    status_code=exc.status_code,
                    detail=detail,
                    headers=headers or None,
                ) from exc

        return custom_handler


# CRUDRouter

class CRUDRouter(APIRouter):
    def __init__(
        self,
        *,
        model:  type[DeclarativeBase],
        schema: type[BaseModel],
        db:     Callable,
        prefix: str,
        create_schema:  type[BaseModel] | None = None,
        update_schema:  type[BaseModel] | None = None,
        filterset:         FilterSet       | None = None,
        soft_delete:       bool            = False,
        audit_trail:       bool            = False,
        hooks:             CRUDHooks       | None = None,
        cache:             Cache           | None = None,
        cache_endpoints:   list[str]       | None = None,
        rate_limit:        RateLimiter     | None = None,
        webhooks:          WebhookConfig   | None = None,
        field_permissions: FieldPermissions| None = None,
        get_current_user:  Callable        | None = None, 
        tags:         list[str] | None = None,
        dependencies: list      | None = None,
        disable:      list[str] | None = None,
        **kwargs,
    ):
        kwargs.setdefault("route_class", CRUDExceptionRoute)
        super().__init__(
            prefix=prefix,
            tags=tags or [prefix.strip("/").title()],
            dependencies=dependencies or [],
            **kwargs,
        )

        self.model         = model
        self.schema        = schema
        self.cs            = create_schema or schema
        self.us            = update_schema or schema
        self.db_dep        = db
        self.hooks         = hooks or CRUDHooks()
        self.cache         = cache
        self.cache_eps     = set(cache_endpoints or ["list", "get"])
        self.rate_limit    = rate_limit
        self.webhooks      = webhooks
        self.field_perms   = field_permissions
        self.get_user      = get_current_user
        self._use_user_dependency = self._callable_uses_fastapi_depends(get_current_user)
        self.disable       = set(disable or [])
        self.soft_delete   = soft_delete
        self.pk_type       = self._resolve_pk_type(model)
        self.pk_path       = self._resolve_pk_path(self.pk_type)
        if self.soft_delete and not issubclass(model, SoftDeleteMixin):
            raise RuntimeError(
                f"Model '{model.__name__}' must inherit SoftDeleteMixin when soft_delete=True."
            )

        _audit_model = None
        if audit_trail:
            base_model = self._resolve_declarative_base(model)
            try:
                _audit_model = build_audit_log_model(base_model)
            except Exception as exc:
                raise RuntimeError(
                    f"Failed to initialize audit model for {model.__name__}: {exc}"
                ) from exc

        self.repo = CRUDRepository(
            model,
            soft_delete      = soft_delete,
            filterset        = filterset or FilterSet(),
            audit_trail      = audit_trail,
            audit_log_model  = _audit_model,
        )

        self._register_routes()

    @staticmethod
    def _resolve_declarative_base(model: type[DeclarativeBase]) -> type[DeclarativeBase]:
        for cls in model.__mro__[1:]:
            if hasattr(cls, "registry") and hasattr(cls, "metadata"):
                return cls
        raise RuntimeError(
            f"Could not resolve declarative base for model '{model.__name__}'."
        )

    @staticmethod
    def _resolve_pk_type(model: type[DeclarativeBase]) -> type:
        pk_col = model.__mapper__.primary_key[0]
        try:
            pk_type = pk_col.type.python_type
        except Exception:
            return str
        if pk_type in (int, str, UUID):
            return pk_type
        return str

    @staticmethod
    def _resolve_pk_path(pk_type: type) -> str:
        return "/{pk}"

    @staticmethod
    def _annotation_uses_depends(annotation: Any) -> bool:
        if get_origin(annotation) is Annotated:
            for arg in get_args(annotation)[1:]:
                if isinstance(arg, DependsParam):
                    return True
        return False

    @classmethod
    def _callable_uses_fastapi_depends(cls, fn: Callable | None) -> bool:
        if fn is None:
            return False
        try:
            sig = inspect.signature(fn)
        except (TypeError, ValueError):
            return False

        for param in sig.parameters.values():
            if isinstance(param.default, DependsParam):
                return True
            if param.annotation is not inspect._empty and cls._annotation_uses_depends(param.annotation):
                return True
        return False

    async def _check_rate(self, request: Request) -> None:
        if self.rate_limit:
            await self.rate_limit.check(request)

    async def _get_user_str(self, request: Request) -> str | None:
        if not self.get_user or self._use_user_dependency:
            return None
        try:
            user = self.get_user(request)
            if asyncio.iscoroutine(user):
                user = await user
            return str(user) if user else None
        except Exception:
            return None

    @staticmethod
    def _user_to_str(user: Any) -> str | None:
        return str(user) if user is not None else None
        
    def _get_request_meta(self, request:Request) -> tuple[str | None, str |None]:
        ip = None
        forwarded = request.headers.get("x-forwarded-for")
        if forwarded:
            ip = forwarded.split(",")[0].strip()
        elif request.client:
            ip = request.client.host
        
        ua = request.headers.get("user-agent")
        return ip, ua

    def _apply_field_perms(self, data: dict, role: str) -> dict:
        if not self.field_perms:
            return data
        return self.field_perms.filter_response(data, role)

    def _filter_write_data(self, data: dict[str, Any], role: str) -> dict[str, Any]:
        if not self.field_perms:
            return data

        filtered = self.field_perms.filter_write(data, role)
        forbidden = sorted(set(data) - set(filtered))
        if forbidden:
            raise PermissionDeniedException(
                f"Role '{role}' cannot write fields: {', '.join(forbidden)}"
            )
        return filtered

    def _filter_obj_for_role(self, obj: Any, role: str) -> dict[str, Any]:
        data = _obj_to_dict(obj)
        return self._apply_field_perms(data, role)

    def _filter_list_for_role(self, items: list[Any], role: str) -> list[dict[str, Any]]:
        return [self._filter_obj_for_role(item, role) for item in items]

    @staticmethod
    def _json(data: Any, status_code: int = 200) -> JSONResponse:
        return JSONResponse(content=jsonable_encoder(data), status_code=status_code)

    def _get_role(self, request: Request) -> str:
        try:
            return getattr(request.state, "role", None) or "user"
        except Exception:
            return "user"

    async def _cache_get(self, key: str) -> Any | None:
        if not self.cache:
            return None
        return await self.cache.get(key)

    async def _cache_set(self, key: str, value: Any) -> None:
        if self.cache:
            await self.cache.set(key, value)

    async def _cache_invalidate(self) -> None:
        if self.cache:
            await self.cache.invalidate_model(self.model.__tablename__)

    async def _emit(self, event: str, record_id: Any, obj: Any) -> None:
        if self.webhooks:
            try:
                task = asyncio.create_task(
                    self.webhooks.dispatch(
                        event=f"{self.model.__name__.lower()}.{event}",
                        table=self.model.__tablename__,
                        record_id=record_id,
                        data=_obj_to_dict(obj),
                    )
                )
            except RuntimeError:
                return
            task.add_done_callback(self._on_emit_done)

    @staticmethod
    def _on_emit_done(task: asyncio.Task[Any]) -> None:
        try:
            task.result()
        except Exception:
            router_logger.exception("Webhook dispatch task failed.")

    def _register_routes(self) -> None:
        repo    = self.repo
        model   = self.model
        schema  = self.schema
        cs      = self.cs
        us      = self.us
        db_dep  = self.db_dep
        hooks   = self.hooks
        disable = self.disable
        pk_type = self.pk_type
        pk_path = self.pk_path
        user_dep = (
            Depends(self.get_user)
            if self.get_user and self._use_user_dependency
            else Depends(lambda: None)
        )

        @self.get("", response_model=PageResponse[schema], summary=f"List {model.__name__}")
        async def list_items(
            request:     Request,
            page_params: PageParams = Depends(),
            db:          AsyncSession = Depends(db_dep),
        ):
            await self._check_rate(request)
            role = self._get_role(request)
            cache_key = Cache.make_key(
                f"crud:{model.__tablename__}:list",
                page=page_params.page, size=page_params.size,
                q=str(request.query_params),
                role=role,
            )
            if "list" in self.cache_eps:
                cached = await self._cache_get(cache_key)
                if cached:
                    return self._json(cached)

            filter_params = request.query_params
            result = await repo.list(db, params=page_params, filter_params=filter_params)

            if self.field_perms:
                payload = result.model_dump()
                payload["items"] = self._filter_list_for_role(result.items, role)
                if "list" in self.cache_eps:
                    await self._cache_set(cache_key, payload)
                return self._json(payload)

            if "list" in self.cache_eps:
                await self._cache_set(cache_key, result.model_dump())

            return result

        if self.soft_delete and "deleted" not in disable:
            @self.get("/deleted", response_model=PageResponse[schema], summary=f"Soft-deleted {model.__name__}")
            async def list_deleted(
                request:     Request,
                page_params: PageParams = Depends(),
                db:          AsyncSession = Depends(db_dep),
            ):
                await self._check_rate(request)
                result = await repo.list_deleted(db, params=page_params)
                if self.field_perms:
                    role = self._get_role(request)
                    payload = result.model_dump()
                    payload["items"] = self._filter_list_for_role(result.items, role)
                    return self._json(payload)
                return result

        if "export" not in disable:
            @self.get("/export", summary=f"Export {model.__name__}")
            async def export(
                request: Request,
                fmt:     str = Query(default="csv", description="csv | xlsx"),
                page:    int = Query(default=1, ge=1),
                size:    int = Query(default=1000, ge=1, le=10_000),
                db:      AsyncSession = Depends(db_dep),
            ):
                await self._check_rate(request)
                role = self._get_role(request)
                filter_items = [
                    (k, v)
                    for k, v in request.query_params.multi_items()
                    if k not in {"fmt", "page", "size"}
                ]
                filter_params = QueryParams(filter_items)
                result = await repo.list(
                    db,
                    params=PageParams(page=page, size=size),
                    filter_params=filter_params,
                )
                name   = model.__tablename__
                items: list[Any]
                if self.field_perms:
                    items = self._filter_list_for_role(result.items, role)
                else:
                    items = result.items
                if fmt == "xlsx":
                    return export_excel(name, items)
                return export_csv(name, items)

        @self.get(pk_path, response_model=schema, summary=f"Get {model.__name__}")
        async def get_one(
            pk:      pk_type,
            request: Request,
            db:      AsyncSession = Depends(db_dep),
        ):
            await self._check_rate(request)
            role = self._get_role(request)
            if "get" in self.cache_eps:
                key = Cache.make_key(
                    f"crud:{model.__tablename__}:get:{pk}",
                    role=role,
                )
                cached = await self._cache_get(key)
                if cached:
                    return self._json(cached)

            obj = await repo.get(db, pk)

            if self.field_perms:
                payload = self._filter_obj_for_role(obj, role)
                if "get" in self.cache_eps:
                    key = Cache.make_key(
                        f"crud:{model.__tablename__}:get:{pk}",
                        role=role,
                    )
                    await self._cache_set(key, payload)
                return self._json(payload)

            if "get" in self.cache_eps:
                key = Cache.make_key(
                    f"crud:{model.__tablename__}:get:{pk}",
                    role=role,
                )
                await self._cache_set(key, _obj_to_dict(obj))

            return obj

        @self.post("", response_model=schema, status_code=201, summary=f"Create {model.__name__}")
        async def create_one(
            payload: cs,
            request: Request,
            db:      AsyncSession = Depends(db_dep),
            current_user: Any = user_dep,
        ):
            await self._check_rate(request)
            role = self._get_role(request)
            changed_by = (
                self._user_to_str(current_user)
                if self._use_user_dependency
                else await self._get_user_str(request)
            )
            ip, ua = self._get_request_meta(request)
            write_data = self._filter_write_data(
                payload.model_dump(exclude_unset=True),
                role,
            )

            await _hook(hooks.before_create, db, payload)
            obj = await repo.create(db, write_data, changed_by=changed_by, ip_address=ip, user_agent=ua)
            await _hook(hooks.after_create, db, obj)
            await self._cache_invalidate()
            await self._emit("created", self.repo._pk_val(obj), obj)
            if self.field_perms:
                return self._json(self._filter_obj_for_role(obj, role), status_code=201)
            return obj

        @self.put(pk_path, response_model=schema, summary=f"Update {model.__name__}")
        async def update_one(
            pk:      pk_type,
            payload: us,
            request: Request,
            db:      AsyncSession = Depends(db_dep),
            version: Optional[int] = Query(default=None, description="version_id for optimistic locking"),
            current_user: Any = user_dep,
        ):
            await self._check_rate(request)
            role = self._get_role(request)
            changed_by = (
                self._user_to_str(current_user)
                if self._use_user_dependency
                else await self._get_user_str(request)
            )
            ip, ua = self._get_request_meta(request)
            write_data = self._filter_write_data(payload.model_dump(), role)
            await _hook(hooks.before_update, db, payload)
            obj = await repo.update(
                db, pk, write_data,
                expected_version=version, changed_by=changed_by,
                ip_address=ip, user_agent=ua
            )
            await _hook(hooks.after_update, db, obj)
            await self._cache_invalidate()
            await self._emit("updated", pk, obj)
            if self.field_perms:
                return self._json(self._filter_obj_for_role(obj, role))
            return obj

        @self.patch(pk_path, response_model=schema, summary=f"Patch {model.__name__}")
        async def patch_one(
            pk:      pk_type,
            payload: us,
            request: Request,
            db:      AsyncSession = Depends(db_dep),
            current_user: Any = user_dep,
        ):
            await self._check_rate(request)
            role = self._get_role(request)
            changed_by = (
                self._user_to_str(current_user)
                if self._use_user_dependency
                else await self._get_user_str(request)
            )
            ip, ua = self._get_request_meta(request)
            write_data = self._filter_write_data(
                payload.model_dump(exclude_unset=True),
                role,
            )
            await _hook(hooks.before_update, db, payload)
            obj = await repo.update(
                db, pk, write_data,
                partial=True, changed_by=changed_by,
                ip_address=ip, user_agent=ua,
            )
            await _hook(hooks.after_update, db, obj)
            await self._cache_invalidate()
            await self._emit("updated", pk, obj)
            if self.field_perms:
                return self._json(self._filter_obj_for_role(obj, role))
            return obj

        @self.delete(pk_path, status_code=204, summary=f"Delete {model.__name__}")
        async def delete_one(
            pk:      pk_type,
            request: Request,
            db:      AsyncSession = Depends(db_dep),
            current_user: Any = user_dep,
        ):
            await self._check_rate(request)
            changed_by = (
                self._user_to_str(current_user)
                if self._use_user_dependency
                else await self._get_user_str(request)
            )
            ip, ua = self._get_request_meta(request)
            await _hook(hooks.before_delete, db, pk)
            obj = await repo.delete(db, pk, changed_by=changed_by, ip_address=ip, user_agent = ua)
            await _hook(hooks.after_delete, db, obj)
            await self._cache_invalidate()
            await self._emit("deleted", pk, obj)
            return Response(status_code=204)

        if self.soft_delete and "restore" not in disable:
            @self.post(f"{pk_path}/restore", response_model=schema, summary=f"Restore {model.__name__}")
            async def restore_one(
                pk:      pk_type,
                request: Request,
                db:      AsyncSession = Depends(db_dep),
                current_user: Any = user_dep,
            ):
                await self._check_rate(request)
                role = self._get_role(request)
                changed_by = (
                    self._user_to_str(current_user)
                    if self._use_user_dependency
                    else await self._get_user_str(request)
                )
                ip, ua = self._get_request_meta(request)
                await _hook(hooks.before_restore, db, pk)
                obj = await repo.restore(db, pk, changed_by=changed_by, ip_address=ip, user_agent=ua)
                await _hook(hooks.after_restore, db, obj)
                await self._cache_invalidate()
                await self._emit("restored", pk, obj)
                if self.field_perms:
                    return self._json(self._filter_obj_for_role(obj, role))
                return obj

        if "bulk" not in disable:
            @self.post("/bulk", response_model=list[schema], status_code=201, summary=f"Bulk create {model.__name__}")
            async def bulk_create(
                payload: list[cs],
                request: Request,
                db:      AsyncSession = Depends(db_dep),
                current_user: Any = user_dep,
            ):
                await self._check_rate(request)
                role = self._get_role(request)
                changed_by = (
                    self._user_to_str(current_user)
                    if self._use_user_dependency
                    else await self._get_user_str(request)
                )
                ip, ua = self._get_request_meta(request)
                items = [
                    self._filter_write_data(p.model_dump(exclude_unset=True), role)
                    for p in payload
                ]
                objs  = await repo.bulk_create(db, items, changed_by=changed_by, ip_address=ip, user_agent=ua)
                await self._cache_invalidate()
                if self.field_perms:
                    return self._json(self._filter_list_for_role(objs, role), status_code=201)
                return objs

        if "import" not in disable:
            @self.post("/import", summary=f"Import {model.__name__} from CSV/XLSX")
            async def import_file(
                request: Request,
                file:    UploadFile = File(...),
                db:      AsyncSession = Depends(db_dep),
                current_user: Any = user_dep,
            ):
                await self._check_rate(request)
                role = self._get_role(request)
                changed_by = (
                    self._user_to_str(current_user)
                    if self._use_user_dependency
                    else await self._get_user_str(request)
                )
                model_fields = [c.key for c in model.__mapper__.column_attrs]
                valid, errors = await import_csv_or_excel(file, model_fields)
                if self.field_perms and valid:
                    filtered_valid: list[dict[str, Any]] = []
                    for idx, row in enumerate(valid, start=2):
                        try:
                            filtered_valid.append(
                                self._filter_write_data(dict(row), role)
                            )
                        except PermissionDeniedException as exc:
                            errors.append({"row": idx, "error": exc.detail})
                    valid = filtered_valid

                created = []
                if valid:
                    try:
                        created = await repo.bulk_create(
                            db, valid, changed_by=changed_by
                        )
                    except Exception as exc:
                        return {"created": 0, "errors": [{"error": str(exc)}]}
                await self._cache_invalidate()
                return {
                    "created": len(created),
                    "errors":  errors,
                    "total_rows": len(valid) + len(errors),
                }
