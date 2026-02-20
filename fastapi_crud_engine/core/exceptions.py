from __future__ import annotations

from typing import Any


class CRUDException(Exception):
    status_code: int = 400
    default_detail: str = "Request failed."

    def __init__(self, detail: str | None = None) -> None:
        self.detail = detail or self.default_detail
        super().__init__(self.detail)


class FilterError(ValueError):
    """Raised when filter params are invalid."""


class NotFoundException(CRUDException):
    status_code = 404
    default_detail = "Resource not found."


class PermissionDeniedException(CRUDException):
    status_code = 403
    default_detail = "Permission denied."


class OptimisticLockException(CRUDException):
    status_code = 409
    default_detail = "Record has been modified by another request."


class BulkOperationException(CRUDException):
    status_code = 400
    default_detail = "Bulk operation failed."

    def __init__(self, errors: list[dict[str, Any]], detail: str | None = None) -> None:
        self.errors = errors
        super().__init__(detail or self.default_detail)


class RateLimitException(CRUDException):
    status_code = 429
    default_detail = "Rate limit exceeded."

    def __init__(self, retry_after: int, detail: str | None = None) -> None:
        self.retry_after = max(1, int(retry_after))
        super().__init__(detail or f"{self.default_detail} Retry after {self.retry_after}s.")
