from __future__ import annotations

from fastapi_crud_engine.core.permissions import FieldPermissions


def test_filter_response_respects_hidden_and_role_rules() -> None:
    perms = FieldPermissions(
        read={"admin": "__all__", "user": ["id", "name"]},
        hidden_by_default=["secret"],
    )

    data = {"id": 1, "name": "alice", "secret": "x"}

    assert perms.filter_response(data, "admin") == {"id": 1, "name": "alice"}
    assert perms.filter_response(data, "user") == {"id": 1, "name": "alice"}
    assert perms.filter_response(data, "unknown") == {"id": 1, "name": "alice"}


def test_filter_write_returns_allowed_subset() -> None:
    perms = FieldPermissions(write={"admin": "__all__", "user": ["name"]})

    data = {"name": "alice", "secret": "x"}

    assert perms.filter_write(data, "admin") == data
    assert perms.filter_write(data, "user") == {"name": "alice"}
    assert perms.filter_write(data, "guest") == {}


def test_can_read_and_can_write_field() -> None:
    perms = FieldPermissions(
        read={"user": ["id"]},
        write={"user": ["name"]},
        hidden_by_default=["secret"],
    )

    assert perms.can_read_field("id", "user") is True
    assert perms.can_read_field("name", "user") is False
    assert perms.can_read_field("secret", "user") is False

    assert perms.can_write_field("name", "user") is True
    assert perms.can_write_field("secret", "user") is False
    assert perms.can_write_field("name", "guest") is False
