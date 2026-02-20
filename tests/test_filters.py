from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

import pytest
from fastapi import HTTPException
from sqlalchemy import DateTime, ForeignKey, Integer, Numeric, String, select
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from fastapi_crud_engine.core.filters import FilterSet


class Base(DeclarativeBase):
    pass


class Team(Base):
    __tablename__ = "teams"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)


class FilterUser(Base):
    __tablename__ = "filter_users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    balance: Mapped[Decimal] = mapped_column(Numeric(10, 2), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    team_id: Mapped[int | None] = mapped_column(ForeignKey("teams.id"), nullable=True)
    team: Mapped[Team | None] = relationship()


class _ParamsWithGetList(dict[str, Any]):
    def getlist(self, key: str) -> list[Any]:
        value = self.get(key)
        if value is None:
            return []
        if isinstance(value, list):
            return value
        return [value]


def test_apply_combines_multiple_filters() -> None:
    fs = FilterSet(
        fields=["status"],
        search_fields=["name"],
        icontains_fields=["name"],
        range_fields=["id"],
        in_fields=["status"],
        nullable_fields=["deleted_at"],
        ordering_fields=["id", "name"],
    )

    params = {
        "status": "active",
        "search": "ali",
        "name__icontains": "li",
        "id__gte": "1",
        "id__lte": "9",
        "status__in": "active,pending",
        "deleted_at__isnull": "true",
        "ordering": "-name",
    }

    query = fs.apply(select(FilterUser), FilterUser, params)

    assert len(query._where_criteria) == 7
    assert len(query._order_by_clauses) == 1


def test_apply_invalid_boolean_raises_http_400() -> None:
    fs = FilterSet(nullable_fields=["deleted_at"])

    with pytest.raises(HTTPException) as exc:
        fs.apply(select(FilterUser), FilterUser, {"deleted_at__isnull": "maybe"})

    assert exc.value.status_code == 400
    assert "Invalid boolean value" in str(exc.value.detail)


def test_coerce_handles_numeric_datetime_and_date() -> None:
    fs = FilterSet()

    assert fs._coerce(FilterUser.balance, "12.50") == Decimal("12.50")
    dt = fs._coerce(FilterUser.created_at, "2024-01-01T12:34:56+00:00")
    assert isinstance(dt, datetime)

    assert fs._coerce(FilterUser.status, "x") == "x"


def test_get_param_values_supports_mapping_with_getlist() -> None:
    fs = FilterSet()
    params = _ParamsWithGetList({"status__in": ["active", "pending"]})

    values = fs._get_param_values(params, "status__in")

    assert values == ["active", "pending"]


def test_get_column_supports_relationship_path() -> None:
    fs = FilterSet()

    col = fs._get_column(FilterUser, "team.name")

    assert col is not None
    assert col.key == "name"
