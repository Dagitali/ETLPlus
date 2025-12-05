"""
:mod:`etlplus.api.types` module.

Centralized type aliases and ``TypedDict``-based configurations used in the
:mod:`etlplus.api` package.

Examples
--------
>>> from etlplus.api import PaginationConfig
>>> pg: PaginationConfig = {"type": "page", "page_size": 100}
>>> from etlplus.api import RetryPolicy
>>> rp: RetryPolicy = {"max_attempts": 3, "backoff": 0.5}
"""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from typing import NotRequired
from typing import TypedDict

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Typed Dicts
    'RetryPolicy',
]


# SECTION: TYPE ALIASES ===================================================== #


type Headers = Mapping[str, str]
type Params = Mapping[str, Any]
type Url = str


# SECTION: TYPED DICTS ====================================================== #


class RetryPolicy(TypedDict):
    """
    Optional retry policy for HTTP requests.

    Summary
    -------
    Controls exponential backoff with jitter (applied externally) and retry
    eligibility by HTTP status code.

    Attributes
    ----------
    max_attempts : NotRequired[int]
        Maximum number of attempts (including the first). If omitted, a default
        may be applied by callers.
    backoff : NotRequired[float]
        Base backoff seconds; attempt ``n`` sleeps ``backoff * 2**(n-1)``
        before retrying.
    retry_on : NotRequired[list[int]]
        HTTP status codes that should trigger a retry.

    Examples
    --------
    >>> rp: RetryPolicy = {
    ...     'max_attempts': 5,
    ...     'backoff': 0.5,
    ...     'retry_on': [429, 502, 503, 504],
    ... }
    """

    # -- Attributes -- #

    max_attempts: NotRequired[int]
    backoff: NotRequired[float]
    retry_on: NotRequired[list[int]]
