from __future__ import annotations
import json
from datetime import datetime, timezone
from typing import Any, Generic, Mapping, TypeVar

from sqlalchemy import func, inspect, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import DeclarativeBase, RelationshipProperty

from core.audit import write_audit_log
from core.exceptions import (
    BulkOperationException, NotFoundException, OptimisticLockException,
)
from core.filters import FilterSet
from core.mixins import OptimisticLockMixin, SoftDeleteMixin
from core.pagination import PageParams, PageResponse

ModelType = TypeVar("ModelType", bound=DeclarativeBase)


class CRUDRepository(Generic[ModelType]):

    def __init__(
        self,
        model: type[ModelType],
        *,
        soft_delete:      bool       = False,
        filterset:        FilterSet  | None = None,
        audit_trail:      bool       = False,
        audit_log_model:  type | None = None,
    ):
        self.model           = model
        self.soft_delete     = soft_delete
        self.filterset       = filterset or FilterSet()
        self.audit_trail     = audit_trail
        self.audit_log_model = audit_log_model

    def _base_query(self):
        q = select(self.model)
        if self.soft_delete and issubclass(self.model, SoftDeleteMixin):
            q = q.where(self.model.deleted_at.is_(None))
        return q

    @property
    def _pk(self):
        return self.model.__mapper__.primary_key[0]

    def _pk_val(self, obj) -> Any:
        return getattr(obj, self._pk.name)

    def _snapshot(self, obj) -> dict:
        return {c.key: getattr(obj, c.key) for c in obj.__mapper__.column_attrs}

    def _get_relationships(self) -> dict[str, RelationshipProperty]:
        return {
            name: rel
            for name, rel in inspect(self.model).relationships.items()
        }

    # ── READ ───────────────────────────────────────────────────────────────

    async def get(self, db: AsyncSession, pk: Any) -> ModelType:
        q      = self._base_query().where(self._pk == pk)
        result = await db.execute(q)
        obj    = result.scalar_one_or_none()
        if obj is None:
            raise NotFoundException(f"{self.model.__name__} id={pk} not found")
        return obj

    async def get_deleted(self, db: AsyncSession, pk: Any) -> ModelType:
        """Get a soft-deleted record by pk."""
        q      = select(self.model).where(self._pk == pk)
        result = await db.execute(q)
        obj    = result.scalar_one_or_none()
        if obj is None:
            raise NotFoundException(f"{self.model.__name__} id={pk} not found")
        return obj

    async def list(
        self,
        db:            AsyncSession,
        *,
        params:        PageParams | None = None,
        filter_params: Mapping[str, Any] | None = None,
    ) -> PageResponse[ModelType]:
        q = self._base_query()
        if filter_params:
            q = self.filterset.apply(q, self.model, filter_params)

        total_q = select(func.count()).select_from(q.subquery())
        total   = (await db.execute(total_q)).scalar_one()

        if params:
            q = q.offset(params.offset).limit(params.size)

        items = list((await db.execute(q)).scalars().all())
        p     = params or PageParams(page=1, size=total or 1)
        return PageResponse.create(items, total, p)

    async def list_deleted(
        self,
        db: AsyncSession,
        *,
        params: PageParams | None = None,
    ) -> PageResponse[ModelType]:
        """List only soft-deleted records."""
        q = select(self.model).where(self.model.deleted_at.isnot(None))
        total_q = select(func.count()).select_from(q.subquery())
        total   = (await db.execute(total_q)).scalar_one()
        if params:
            q = q.offset(params.offset).limit(params.size)
        items = list((await db.execute(q)).scalars().all())
        p     = params or PageParams(page=1, size=total or 1)
        return PageResponse.create(items, total, p)

    # ── CREATE ─────────────────────────────────────────────────────────────

    async def create(
        self,
        db:         AsyncSession,
        data:       dict[str, Any],
        *,
        changed_by: str | None = None,
    ) -> ModelType:
        # Handle nested creates (relationships)
        data, nested = self._extract_nested(data)

        obj = self.model(**data)
        db.add(obj)
        await db.flush()

        # Create nested objects
        await self._handle_nested_create(db, obj, nested)

        if self.audit_trail and self.audit_log_model:
            await write_audit_log(
                db, audit_log_model=self.audit_log_model,
                table_name=self.model.__tablename__,
                record_id=self._pk_val(obj),
                action="CREATE", new_obj=obj, changed_by=changed_by,
            )

        await db.commit()
        await db.refresh(obj)
        return obj

    # ── BULK CREATE ────────────────────────────────────────────────────────

    async def bulk_create(
        self,
        db:         AsyncSession,
        items:      list[dict[str, Any]],
        *,
        changed_by: str | None = None,
    ) -> list[ModelType]:
        errors:  list[dict] = []
        created: list[ModelType] = []

        for i, data in enumerate(items):
            try:
                clean, nested = self._extract_nested(data)
                obj = self.model(**clean)
                db.add(obj)
                await db.flush()
                await self._handle_nested_create(db, obj, nested)
                created.append(obj)
            except Exception as exc:
                errors.append({"index": i, "data": data, "error": str(exc)})

        if errors:
            await db.rollback()
            raise BulkOperationException(errors)

        if self.audit_trail and self.audit_log_model:
            for obj in created:
                await write_audit_log(
                    db, audit_log_model=self.audit_log_model,
                    table_name=self.model.__tablename__,
                    record_id=self._pk_val(obj),
                    action="CREATE", new_obj=obj, changed_by=changed_by,
                )

        await db.commit()
        for obj in created:
            await db.refresh(obj)
        return created

    # ── UPDATE ─────────────────────────────────────────────────────────────

    async def update(
        self,
        db:               AsyncSession,
        pk:               Any,
        data:             dict[str, Any],
        *,
        partial:          bool = False,
        expected_version: int | None = None,
        changed_by:       str | None = None,
    ) -> ModelType:
        obj = await self.get(db, pk)

        # Optimistic locking
        if expected_version is not None and isinstance(obj, OptimisticLockMixin):
            if obj.version_id != expected_version:
                raise OptimisticLockException()
            data = {**data, "version_id": expected_version + 1}

        old_snap = self._snapshot(obj)

        # Handle nested updates
        data, nested = self._extract_nested(data)

        for key, value in data.items():
            if partial and value is None:
                continue
            if hasattr(obj, key):
                setattr(obj, key, value)

        await self._handle_nested_update(db, obj, nested)
        await db.flush()

        if self.audit_trail and self.audit_log_model:
            await write_audit_log(
                db, audit_log_model=self.audit_log_model,
                table_name=self.model.__tablename__,
                record_id=pk, action="UPDATE",
                old_obj=_DictSnapshot(old_snap, obj.__mapper__),
                new_obj=obj, changed_by=changed_by,
            )

        await db.commit()
        await db.refresh(obj)
        return obj

    # ── DELETE ─────────────────────────────────────────────────────────────

    async def delete(
        self,
        db:         AsyncSession,
        pk:         Any,
        *,
        changed_by: str | None = None,
    ) -> ModelType:
        obj = await self.get(db, pk)

        if self.audit_trail and self.audit_log_model:
            await write_audit_log(
                db, audit_log_model=self.audit_log_model,
                table_name=self.model.__tablename__,
                record_id=pk, action="DELETE",
                old_obj=obj, changed_by=changed_by,
            )

        if self.soft_delete and isinstance(obj, SoftDeleteMixin):
            obj.soft_delete()
            await db.commit()
            await db.refresh(obj)
        else:
            await db.delete(obj)
            await db.commit()

        return obj

    # ── RESTORE ────────────────────────────────────────────────────────────

    async def restore(
        self,
        db:         AsyncSession,
        pk:         Any,
        *,
        changed_by: str | None = None,
    ) -> ModelType:
        obj = await self.get_deleted(db, pk)
        if not isinstance(obj, SoftDeleteMixin):
            raise ValueError(f"{self.model.__name__} does not support soft delete")

        obj.restore()

        if self.audit_trail and self.audit_log_model:
            await write_audit_log(
                db, audit_log_model=self.audit_log_model,
                table_name=self.model.__tablename__,
                record_id=pk, action="RESTORE",
                new_obj=obj, changed_by=changed_by,
            )

        await db.commit()
        await db.refresh(obj)
        return obj

    # ── NESTED helpers ─────────────────────────────────────────────────────

    def _extract_nested(self, data: dict) -> tuple[dict, dict]:
        """Separate scalar fields from relationship dicts/lists."""
        relationships = self._get_relationships()
        nested = {}
        clean  = {}
        for key, value in data.items():
            if key in relationships and isinstance(value, (dict, list)):
                nested[key] = value
            else:
                clean[key] = value
        return clean, nested

    async def _handle_nested_create(
        self,
        db:     AsyncSession,
        parent: Any,
        nested: dict,
    ) -> None:
        """Create nested related objects atomically."""
        relationships = self._get_relationships()
        pk_name = self._pk.name

        for rel_name, rel_data in nested.items():
            rel_info  = relationships.get(rel_name)
            if rel_info is None:
                continue
            child_model = rel_info.mapper.class_

            if isinstance(rel_data, list):
                # One-to-many: create each child
                for child_data in rel_data:
                    # Inject FK automatically
                    fk_col = _find_fk_col(rel_info, parent.__class__)
                    if fk_col:
                        child_data[fk_col] = getattr(parent, pk_name)
                    child = child_model(**child_data)
                    db.add(child)
            elif isinstance(rel_data, dict):
                # Many-to-one: create or get parent
                child = child_model(**rel_data)
                db.add(child)
                await db.flush()
                # Link FK on parent
                fk_col = _find_fk_col_reverse(rel_info, child_model)
                if fk_col:
                    setattr(parent, fk_col, _get_pk(child))

        if nested:
            await db.flush()

    async def _handle_nested_update(
        self,
        db:     AsyncSession,
        parent: Any,
        nested: dict,
    ) -> None:
        """Update nested related objects."""
        relationships = self._get_relationships()
        pk_name = self._pk.name

        for rel_name, rel_data in nested.items():
            rel_info    = relationships.get(rel_name)
            if rel_info is None:
                continue
            child_model = rel_info.mapper.class_

            if isinstance(rel_data, list):
                for child_data in rel_data:
                    child_pk = child_data.get(_get_pk_name(child_model))
                    if child_pk:
                        # Update existing
                        child_q = await db.execute(
                            select(child_model).where(
                                child_model.__mapper__.primary_key[0] == child_pk
                            )
                        )
                        child = child_q.scalar_one_or_none()
                        if child:
                            for k, v in child_data.items():
                                if hasattr(child, k):
                                    setattr(child, k, v)
                    else:
                        # Create new
                        fk_col = _find_fk_col(rel_info, parent.__class__)
                        if fk_col:
                            child_data[fk_col] = getattr(parent, pk_name)
                        db.add(child_model(**child_data))


# ── Helpers ───────────────────────────────────────────────────────────────

def _find_fk_col(rel_info, parent_class) -> str | None:
    """Find the FK column name on the child model that points to parent."""
    for local, remote in rel_info.synchronize_pairs:
        if remote.table == parent_class.__table__:
            return local.name
    return None


def _find_fk_col_reverse(rel_info, child_class) -> str | None:
    for local, remote in rel_info.synchronize_pairs:
        return local.name
    return None


def _get_pk(obj) -> Any:
    return getattr(obj, obj.__mapper__.primary_key[0].name)


def _get_pk_name(model: type) -> str:
    return model.__mapper__.primary_key[0].name


class _DictSnapshot:
    """Thin wrapper to make a dict look like a mapped object for audit serialization."""
    def __init__(self, data: dict, mapper):
        self.__mapper__ = mapper
        for k, v in data.items():
            setattr(self, k, v)
