"""
etlplus.api.errors
==================

Exception types with rich context for debugging REST API failures.

Summary
-------
- :class:`ApiRequestError`: base error for HTTP request failures, including
    the URL, status code, number of attempts, retry flag, and retry policy.
- :class:`ApiAuthError`: specialization for authentication/authorization
    failures (e.g., 401/403).
- :class:`PaginationError`: adds ``page`` context for failures during
    pagination.

Examples
--------
>>> try:
...     client.paginate("list", pagination={"type": "page", "page_size": 50})
... except ApiAuthError as e:
...     print("auth failed", e.status)
... except PaginationError as e:
...     print("page:", e.page, "attempts:", e.attempts)
... except ApiRequestError as e:
...     print("request failed", e.url)
"""
from __future__ import annotations

from dataclasses import dataclass

import requests  # type: ignore

from .types import RetryPolicy


# SECTION: CLASSES ========================================================== #


@dataclass(slots=True)
class ApiRequestError(requests.RequestException):
    """
    Base error for API request failures with useful context.

    Attributes
    ----------
    url : str
        Absolute URL that was requested.
    status : int | None
        HTTP status code when available.
    attempts : int
        Number of attempts performed.
    retried : bool
        Whether any retry attempts were made.
    retry_policy : RetryPolicy | None
        The retry policy in effect, if any.
    cause : Exception | None
        The original underlying exception.
    """

    # -- Attributes -- #

    url: str
    status: int | None = None
    attempts: int = 1
    retried: bool = False
    retry_policy: RetryPolicy | None = None
    cause: Exception | None = None

    # -- Magic Methods (Object Representation) -- #

    def __str__(self) -> str:  # pragma: no cover - formatting only
        base = f"request failed url={self.url!r} status={self.status}"
        meta = f" attempts={self.attempts} retried={self.retried}"

        return f"ApiRequestError({base}{meta})"


class ApiAuthError(ApiRequestError):
    """
    Authentication/authorization failure (e.g., 401/403).
    """


@dataclass(slots=True)
class PaginationError(ApiRequestError):
    """
    Error raised during pagination with page context.

    Attributes
    ----------
    page : int | None
        The page number (1-based) or request count when applicable.
    """

    # -- Attributes -- #

    page: int | None = None

    # -- Maggic Methods (Object Representation) -- #

    def __str__(self) -> str:  # pragma: no cover - formatting only
        base = super().__str__()

        return f"PaginationError({base} page={self.page})"
