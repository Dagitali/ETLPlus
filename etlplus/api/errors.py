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
    Base error for API request failures with rich context.

    Parameters
    ----------
    url : str
        Absolute URL that was requested.
    status : int | None, optional
        HTTP status code when available.
    attempts : int, optional
        Number of attempts performed (defaults to ``1``).
    retried : bool, optional
        Whether any retry attempts were made.
    retry_policy : RetryPolicy | None, optional
        The retry policy in effect, if any.
    cause : Exception | None, optional
        Original underlying exception.

    Attributes
    ----------
    (Same as parameters; stored for introspection/logging.)

    Examples
    --------
    >>> try:
    ...     raise ApiRequestError(url="https://api.example.com/x", status=500)
    ... except ApiRequestError as e:
    ...     print(e.status, e.attempts)
    500 1
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

    Parameters
    ----------
    page : int | None, optional
        Page number (1-based) or request count when applicable.
    **kwargs
        Remaining keyword arguments forwarded to ``ApiRequestError``.

    Attributes
    ----------
    page : int | None
        Stored page number.
    (See ``ApiRequestError`` for remaining attributes.)

    Examples
    --------
    >>> err = PaginationError(url="u", status=400, page=3)
    >>> str(err).startswith("PaginationError(")
    True
    """

    # -- Attributes -- #

    page: int | None = None

    # -- Maggic Methods (Object Representation) -- #

    def __str__(self) -> str:  # pragma: no cover - formatting only
        base = super().__str__()

        return f"PaginationError({base} page={self.page})"
