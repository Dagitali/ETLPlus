"""
etlplus.api.errors module.

Exception types with rich context for debugging REST API failures.

Summary
-------
Provides subclasses for request errors (``ApiRequestError``), auth failures
(``ApiAuthError``), and pagination errors with page context
(``PaginationError``).

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
        Original underlying exception.

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
    """Authentication/authorization failure (e.g., 401/403)."""


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

    Examples
    --------
    >>> err = PaginationError(url="u", status=400, page=3)
    >>> str(err).startswith("PaginationError(")
    True
    """

    # -- Attributes -- #

    page: int | None = None

    # -- Magic Methods (Object Representation) -- #

    def __str__(self) -> str:  # pragma: no cover - formatting only
        base = super().__str__()

        return f"PaginationError({base} page={self.page})"
