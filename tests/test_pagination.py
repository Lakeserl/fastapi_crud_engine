from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest
from sqlalchemy import select

from fastapi_crud_engine.core.pagination import PageParams, PageResponse, paginate


class _FakeScalarCollection:
    def __init__(self, items: list[Any]) -> None:
        self._items = items

    def all(self) -> list[Any]:
        return self._items


class _FakeResult:
    def __init__(self, *, scalar: Any = None, items: list[Any] | None = None) -> None:
        self._scalar = scalar
        self._items = items or []

    def scalar(self) -> Any:
        return self._scalar

    def scalars(self) -> _FakeScalarCollection:
        return _FakeScalarCollection(self._items)


@dataclass
class _FakeDB:
    results: list[_FakeResult] = field(default_factory=list)

    async def execute(self, _query: Any) -> _FakeResult:
        return self.results.pop(0)


def test_page_params_normalizes_invalid_types() -> None:
    params = PageParams(page="bad", size="bad")  # type: ignore[arg-type]
    assert params.page == 1
    assert params.size == 20
    assert params.offset == 0


def test_page_response_create_sets_navigation_flags() -> None:
    params = PageParams(page=2, size=2)
    page = PageResponse.create(items=[1, 2], total=5, params=params)

    assert page.pages == 3
    assert page.has_prev is True
    assert page.has_next is True


@pytest.mark.asyncio
async def test_paginate_clamps_size_and_returns_response() -> None:
    db = _FakeDB(results=[_FakeResult(scalar=3), _FakeResult(items=["a", "b", "c"])])
    params = PageParams(page=1, size=999)

    response = await paginate(db, select(1), params, max_page_size=2)  # type: ignore[arg-type]

    assert response.total == 3
    assert response.size == 2
    assert response.page == 1
    assert response.pages == 2
    assert response.items == ["a", "b", "c"]
