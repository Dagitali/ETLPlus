"""
:mod:`etlplus.api.types` module.

HTTP-centric type aliases for :mod:`etlplus.api` helpers.

Notes
-----
- Keeps pagination, transport, and higher-level modules decoupled from
    ``typing`` details.
- Uses ``Mapping`` inputs to accept both ``dict`` and mapping-like objects.

Examples
--------
>>> from etlplus.api import Url, Headers, Params
>>> url: Url = 'https://api.example.com/data'
>>> headers: Headers = {'Authorization': 'Bearer token'}
>>> params: Params = {'query': 'search term', 'limit': 50}
"""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from typing import TypedDict

from ..types import JSONData
from ..types import StrAnyMap
from ..types import StrStrMap

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Data Classes
    'RequestOptions',

    # Type Aliases
    'FetchPageCallable',
    'Headers',
    'Params',
    'RateLimitOverrides',
    'Url',

    # Typed Dicts
    'RateLimitOverrideMap',
]


# SECTION: TYPED DICTS ====================================================== #


class RateLimitOverrideMap(TypedDict, total=False):
    """
    Overrides accepted by rate-limit helpers.

    Attributes
    ----------
    sleep_seconds : float | None
        Override for sleep seconds between requests.
    max_per_sec : float | None
        Override for maximum requests per second.
    """

    sleep_seconds: float | None
    max_per_sec: float | None


# SECTION: DATA CLASSES ===================================================== #


@dataclass(slots=True, kw_only=True)
class RequestOptions:
    """Immutable snapshot of per-request options."""

    params: Params | None = None
    headers: Headers | None = None
    timeout: float | int | None = None

    def __post_init__(self) -> None:
        if self.params:
            self.params = dict(self.params)
        if self.headers:
            self.headers = dict(self.headers)

    def as_kwargs(self) -> dict[str, Any]:
        """Convert options into ``requests``-compatible kwargs."""
        kw: dict[str, Any] = {}
        if self.params:
            kw['params'] = dict(self.params)
        if self.headers:
            kw['headers'] = dict(self.headers)
        if self.timeout is not None:
            kw['timeout'] = self.timeout
        return kw

    def with_params(self, params: Params | None) -> RequestOptions:
        """Return a copy with ``params`` replaced while preserving context."""
        return RequestOptions(
            params=dict(params) if params else None,
            headers=self.headers,
            timeout=self.timeout,
        )


# SECTION: TYPE ALIASES ===================================================== #


# HTTP headers represented as a string-to-string mapping.
type Headers = StrStrMap

# Query or body parameters allowing arbitrary JSON-friendly values.
type Params = StrAnyMap

# Fully qualified resource locator consumed by transport helpers.
type Url = str

# Callable signature used by pagination helpers to fetch data pages.
type FetchPageCallable = Callable[
    [Url, RequestOptions, int | None],
    JSONData,
]

# Optional mapping of rate-limit fields to override values.
type RateLimitOverrides = RateLimitOverrideMap | None
