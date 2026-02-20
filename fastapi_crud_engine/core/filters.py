"""filters.py — Advanced FilterSet for fastapi-smart-crud.

Features:
  - exact, search, icontains, range (gte/lte/gt/lt), in, isnull, ordering
  - Type coercion based on SQLAlchemy column.type
  - Accepts Mapping-like params (works with Starlette QueryParams)
  - Safe: only uses predeclared allowed fields

Usage:
    FilterSet(
        fields=["status", "category"],
        search_fields=["name", "email"],
        ordering_fields=["created_at", "name"],
        range_fields=["price", "created_at"],
    )

Generated query params:
    ?status=active
    ?search=john
    ?price__gte=100&price__lte=500
    ?ordering=-created_at,name
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, Mapping

from fastapi import HTTPException
from sqlalchemy import (
    Boolean, Date, DateTime, Float, Integer, Numeric,
    String, Time, func, or_,
)
from sqlalchemy.sql import Select

from .exceptions import FilterError

try:
    from dateutil import parser as _dateutil_parser  # type: ignore
except Exception:
    _dateutil_parser = None

logger = logging.getLogger(__name__)


@dataclass
class FilterSet:
    """
    FilterSet for SQLAlchemy + FastAPI.

    Example:
        fs = FilterSet(
            fields=["role", "status"],
            search_fields=["email", "name"],
            ordering_fields=["created_at", "name"],
            range_fields=["price", "created_at"],
            icontains_fields=["name"],
            in_fields=["status"],
            nullable_fields=["deleted_at"],
            default_ordering="-created_at",
        )

        # In endpoint:
        query = select(User)
        query = fs.apply(query, User, request.query_params)
        result = await paginate(db, query, page_params)  # from pagination.py
    """

    fields:           list[str] = field(default_factory=list)
    search_fields:    list[str] = field(default_factory=list)
    ordering_fields:  list[str] = field(default_factory=list)
    range_fields:     list[str] = field(default_factory=list)
    icontains_fields: list[str] = field(default_factory=list)
    in_fields:        list[str] = field(default_factory=list)
    nullable_fields:  list[str] = field(default_factory=list)

    default_ordering: str | None = None


    def apply(
        self,
        query:  Select,
        model:  type,
        params: Mapping[str, Any],
    ) -> Select:
        try:
            q = query
            q = self._apply_exact(q, model, params)
            q = self._apply_search(q, model, params)
            q = self._apply_icontains(q, model, params)
            q = self._apply_range(q, model, params)
            q = self._apply_in(q, model, params)
            q = self._apply_isnull(q, model, params)
            q = self._apply_ordering(q, model, params)
            return q
        except FilterError as exc:
            raise HTTPException(status_code=400, detail=str(exc))


    def _apply_exact(self, query: Select, model: type, params: Mapping) -> Select:
        for f in self.fields:
            raw = self._get_param(params, f)
            if raw is None:
                continue
            col = self._get_column(model, f)
            if col is None:
                continue
            query = query.where(col == self._coerce(col, raw))
        return query

    def _apply_search(self, query: Select, model: type, params: Mapping) -> Select:
        term = self._get_param(params, "search")
        if not term or not self.search_fields:
            return query
        t       = str(term).lower()
        clauses = []
        for sf in self.search_fields:
            col = self._get_column(model, sf)
            if col is not None:
                clauses.append(func.lower(col.cast(String)).like(f"%{t}%"))
        if clauses:
            query = query.where(or_(*clauses))
        return query

    def _apply_icontains(self, query: Select, model: type, params: Mapping) -> Select:
        for f in self.icontains_fields:
            raw = self._get_param(params, f"{f}__icontains")
            if raw is None:
                continue
            col = self._get_column(model, f)
            if col is None:
                continue
            query = query.where(func.lower(col.cast(String)).like(f"%{str(raw).lower()}%"))
        return query

    def _apply_range(self, query: Select, model: type, params: Mapping) -> Select:
        ops = [("__gte", "__ge__"), ("__lte", "__le__"), ("__gt", "__gt__"), ("__lt", "__lt__")]
        for f in self.range_fields:
            col = self._get_column(model, f)
            if col is None:
                continue
            for suffix, op in ops:
                raw = self._get_param(params, f"{f}{suffix}")
                if raw is not None:
                    query = query.where(getattr(col, op)(self._coerce(col, raw)))
        return query

    def _apply_in(self, query: Select, model: type, params: Mapping) -> Select:
        for f in self.in_fields:
            raw_values = self._get_param_values(params, f"{f}__in")
            if not raw_values:
                continue
            col = self._get_column(model, f)
            if col is None:
                continue
            values = [self._coerce(col, v) for v in self._to_list(raw_values)]
            if values:
                query = query.where(col.in_(values))
        return query

    def _apply_isnull(self, query: Select, model: type, params: Mapping) -> Select:
        for f in self.nullable_fields:
            raw = self._get_param(params, f"{f}__isnull")
            if raw is None:
                continue
            col = self._get_column(model, f)
            if col is None:
                continue
            query = query.where(col.is_(None) if self._parse_bool(raw) else col.isnot(None))
        return query

    def _apply_ordering(self, query: Select, model: type, params: Mapping) -> Select:
        ordering = self._get_param(params, "ordering") or self.default_ordering
        if not ordering or not self.ordering_fields:
            return query
        clauses = []
        for part in str(ordering).split(","):
            part  = part.strip()
            desc  = part.startswith("-")
            fname = part.lstrip("-+")
            if fname not in self.ordering_fields:
                logger.debug("Ordering field '%s' not allowed, skipping.", fname)
                continue
            col = self._get_column(model, fname)
            if col is not None:
                clauses.append(col.desc() if desc else col.asc())
        if clauses:
            query = query.order_by(*clauses)
        return query


    def _get_column(self, model: type, field_name: str):
        for delim in (".", "__"):
            if delim in field_name:
                head, tail = field_name.split(delim, 1)
                parent = getattr(model, head, None)
                if parent is None:
                    logger.warning(
                        "Relation '%s' not found on %s — did you join it?",
                        head, getattr(model, "__name__", model),
                    )
                    return None
                relation = getattr(parent, "property", None)
                mapper = getattr(relation, "mapper", None)
                if mapper is None:
                    logger.warning(
                        "Attribute '%s' on %s is not a relationship, skipping.",
                        head, getattr(model, "__name__", model),
                    )
                    return None
                return self._get_column(mapper.class_, tail)

        col = getattr(model, field_name, None)
        if col is None:
            logger.warning(
                "Field '%s' not found on model %s, skipping.",
                field_name, getattr(model, "__name__", model),
            )
        return col

    def _get_param(self, params: Mapping, key: str) -> Any | None:
        values = self._get_param_values(params, key)
        return values[0] if values else None

    def _get_param_values(self, params: Mapping, key: str) -> list[Any]:
        if hasattr(params, "getlist"):
            vals = params.getlist(key)
            return [v for v in vals if v is not None]

        raw = params.get(key)
        if raw is None:
            return []
        if isinstance(raw, (list, tuple)):
            return [v for v in raw if v is not None]
        return [raw]

    def _to_list(self, raw: Any) -> list[str]:
        if raw is None:
            return []
        if isinstance(raw, (list, tuple)):
            values: list[str] = []
            for item in raw:
                if item is None:
                    continue
                values.extend([p.strip() for p in str(item).split(",") if p.strip()])
            return values
        return [p.strip() for p in str(raw).split(",") if p.strip()]

    def _coerce(self, column: Any, raw: Any) -> Any:
        if raw is None:
            return None
        if isinstance(raw, (int, float, bool, Decimal, datetime, date, time)):
            return raw

        typ = getattr(column, "type", None)
        s   = str(raw)

        try:
            if isinstance(typ, Integer):  return int(s)
            if isinstance(typ, Float):    return float(s)
            if isinstance(typ, Numeric):  return Decimal(s)
            if isinstance(typ, Boolean):  return self._parse_bool(s)
            if isinstance(typ, DateTime):
                return _dateutil_parser.parse(s) if _dateutil_parser else datetime.fromisoformat(s)
            if isinstance(typ, Date):
                return _dateutil_parser.parse(s).date() if _dateutil_parser else date.fromisoformat(s)
            if isinstance(typ, Time):
                return _dateutil_parser.parse(s).time() if _dateutil_parser else time.fromisoformat(s)
        except Exception as exc:
            raise FilterError(
                f"Invalid value '{raw}' for field "
                f"'{getattr(column, 'key', column)}': {exc}"
            ) from exc

        return s  

    @staticmethod
    def _parse_bool(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        normalized = str(value).strip().lower()
        truthy = {"true", "1", "yes", "y", "t"}
        falsy = {"false", "0", "no", "n", "f"}
        if normalized in truthy:
            return True
        if normalized in falsy:
            return False
        raise FilterError(f"Invalid boolean value '{value}'. Use true/false.")
