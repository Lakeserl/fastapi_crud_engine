from __future__ import annotations

import io

import pytest
from sqlalchemy import Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from fastapi_crud_engine.features.export_import import (
    _columns,
    _parse_csv,
    _safe,
    export_csv,
    export_excel,
    import_csv_or_excel,
)


class Base(DeclarativeBase):
    pass


class ExportModel(Base):
    __tablename__ = "export_models"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)


class _FakeUpload:
    def __init__(self, filename: str, content: bytes) -> None:
        self.filename = filename
        self._content = content

    async def read(self) -> bytes:
        return self._content


def test_export_csv_with_dict_items_and_empty_values() -> None:
    resp = export_csv("users", [{"id": 1, "name": "alice"}, {"id": 2, "name": None}])

    assert resp.headers["content-disposition"] == 'attachment; filename="users.csv"'


def test_export_csv_with_empty_items_returns_empty_body() -> None:
    resp = export_csv("users", [])
    assert resp.headers["content-disposition"] == 'attachment; filename="users.csv"'


def test_export_excel_with_dict_items() -> None:
    resp = export_excel("users", [{"id": 1, "name": "alice"}])

    assert resp.headers["content-disposition"] == 'attachment; filename="users.xlsx"'


@pytest.mark.asyncio
async def test_import_csv_or_excel_filters_unknown_fields() -> None:
    content = b"name,unknown\nalice,x\n,\n"
    file = _FakeUpload(filename="users.csv", content=content)

    valid, errors = await import_csv_or_excel(file, ["name"])

    assert valid == [{"name": "alice"}, {"name": ""}]
    assert errors == []


@pytest.mark.asyncio
async def test_import_csv_or_excel_reports_rows_without_recognized_fields() -> None:
    content = b"unknown\nx\n"
    file = _FakeUpload(filename="users.csv", content=content)

    valid, errors = await import_csv_or_excel(file, ["name"])

    assert valid == []
    assert errors == [{"row": 2, "error": "No recognisable fields in row"}]


@pytest.mark.asyncio
async def test_import_excel_reads_rows() -> None:
    openpyxl = pytest.importorskip("openpyxl")

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["id", "name", "ignore"])
    ws.append([1, "alice", "x"])
    ws.append([2, "bob", "y"])

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)

    file = _FakeUpload(filename="users.xlsx", content=buf.getvalue())
    valid, errors = await import_csv_or_excel(file, ["id", "name"])

    assert errors == []
    assert valid == [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]


def test_columns_support_dict_and_orm_object() -> None:
    assert _columns({"a": 1, "b": 2}) == ["a", "b"]
    assert _columns(ExportModel(id=1, name="alice")) == ["id", "name"]

    with pytest.raises(RuntimeError, match="ORM object or dict"):
        _columns("bad")


def test_safe_formats_values() -> None:
    assert _safe(None) == ""
    assert _safe(1) == 1
    assert _safe(True) is True
    assert _safe("x") == "x"


def test_parse_csv_returns_rows() -> None:
    rows = _parse_csv(b"name,age\nalice,1\nbob,2\n")
    assert rows == [{"name": "alice", "age": "1"}, {"name": "bob", "age": "2"}]
