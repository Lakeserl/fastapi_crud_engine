from __future__ import annotations

import pytest

from fastapi_crud_engine.core.exceptions import (
    BulkOperationException,
    CRUDException,
    NotFoundException,
    OptimisticLockException,
    PermissionDeniedException,
    RateLimitException,
)
from fastapi_crud_engine.core.handlers import build_error_payload


def test_crud_exception_uses_default_and_custom_detail() -> None:
    default_exc = CRUDException()
    custom_exc = CRUDException("custom")

    assert default_exc.status_code == 400
    assert default_exc.detail == "Request failed."
    assert str(default_exc) == "Request failed."
    assert custom_exc.detail == "custom"


@pytest.mark.parametrize(
    ("exc_cls", "status_code", "detail"),
    [
        (NotFoundException, 404, "Resource not found."),
        (PermissionDeniedException, 403, "Permission denied."),
        (OptimisticLockException, 409, "Record has been modified by another request."),
    ],
)
def test_specialized_exceptions_have_expected_http_status(
    exc_cls: type[CRUDException],
    status_code: int,
    detail: str,
) -> None:
    exc = exc_cls()
    assert exc.status_code == status_code
    assert exc.detail == detail


def test_bulk_operation_exception_keeps_errors_and_detail() -> None:
    errors = [{"index": 1, "error": "bad row"}]
    exc = BulkOperationException(errors)
    custom = BulkOperationException(errors, detail="bulk failed")

    assert exc.status_code == 400
    assert exc.detail == "Bulk operation failed."
    assert exc.errors == errors
    assert custom.detail == "bulk failed"


def test_rate_limit_exception_normalizes_retry_after() -> None:
    exc = RateLimitException(retry_after=0)

    assert exc.status_code == 429
    assert exc.retry_after == 1
    assert "Retry after 1s." in exc.detail


def test_build_error_payload_for_bulk_exception() -> None:
    exc = BulkOperationException([{"index": 0, "error": "invalid"}])
    detail, headers = build_error_payload(exc)

    assert detail == {
        "message": "Bulk operation failed.",
        "errors": [{"index": 0, "error": "invalid"}],
    }
    assert headers == {}


def test_build_error_payload_for_rate_limit_exception() -> None:
    exc = RateLimitException(retry_after=7)
    detail, headers = build_error_payload(exc)

    assert detail == {"message": "Rate limit exceeded. Retry after 7s."}
    assert headers == {"Retry-After": "7"}
