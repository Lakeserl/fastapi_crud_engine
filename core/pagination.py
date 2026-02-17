from __future__ import annotations
from math import ceil
from typing import Generic, TypeVar
from fastapi import Query
from pydantic import BaseModel, Field
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import Select

T = TypeVar("T")


class PageParams:
    def __init__(
        self,
        page: int = Query(default=1,  ge=1,      description="Page number (1-based)"),
        size: int = Query(default=20, ge=1, le=500, description="Items per page"),
    ):
        self.page   = page
        self.size   = size
        self.offset = (page - 1) * size


class PageResponse(BaseModel, Generic[T]):
    items:    list[T]
    total:    int
    page:     int
    size:     int
    pages:    int = Field(description="Total pages")
    has_next: bool
    has_prev: bool

    @classmethod
    def create(cls, items: list[T], total: int, params: PageParams) -> "PageResponse[T]":
        pages = max(1, -(-total // params.size))  
        return cls(
            items=items,
            total=total,
            page=params.page,
            size=params.size,
            pages=pages,
            has_next=params.page < pages,
            has_prev=params.page > 1,
        )


async def paginate(
    db:     AsyncSession,
    query:  Select,
    params: PageParams,
    *,
    max_page_size: int = 500,
) -> PageResponse:
    size = max(1, min(max_page_size, params.size))
    page = max(1, params.page)

    count_q = select(func.count()).select_from(query.order_by(None).subquery())
    total   = (await db.execute(count_q)).scalar() or 0

    offset = (page - 1) * size
    rows   = await db.execute(query.limit(size).offset(offset))
    items  = rows.scalars().all()

    pages = ceil(total / size) if total else 0

    return PageResponse(
        items=list(items),
        total=total,
        page=page,
        size=size,
        pages=pages,
        has_next=page < pages,
        has_prev=page > 1,
    )