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
from typing import cast

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
    'Url',
]


# SECTION: CONSTANTS ======================================================== #


_UNSET = object()


# SECTION: DATA CLASSES ===================================================== #


@dataclass(frozen=True, kw_only=True, slots=True)
class RequestOptions:
    """
    Immutable snapshot of per-request options.

    Attributes
    ----------
    params : Params | None
        Query or body parameters.
    headers : Headers | None
        HTTP headers.
    timeout : float | None
        Request timeout in seconds.
    """

    # -- Attributes -- #

    params: Params | None = None
    headers: Headers | None = None
    timeout: float | None = None

    # -- Magic Methods (Object Lifecycle) -- #

    def __post_init__(self) -> None:
        if self.params:
            object.__setattr__(self, 'params', dict(self.params))
        if self.headers:
            object.__setattr__(self, 'headers', dict(self.headers))

    # -- Instance Methods -- #

    def as_kwargs(self) -> dict[str, Any]:
        """
        Convert options into ``requests``-compatible kwargs.

        Returns
        -------
        dict[str, Any]
            Keyword arguments for ``requests`` methods.
        """
        kw: dict[str, Any] = {}
        if self.params:
            kw['params'] = dict(self.params)
        if self.headers:
            kw['headers'] = dict(self.headers)
        if self.timeout is not None:
            kw['timeout'] = self.timeout
        return kw

    def evolve(
        self,
        *,
        params: Params | None | object = _UNSET,
        headers: Headers | None | object = _UNSET,
        timeout: float | None | object = _UNSET,
    ) -> RequestOptions:
        """
        Return a copy with the provided fields replaced.

        Parameters
        ----------
        params : Params | None | object, optional
            Replacement params mapping. ``None`` clears params. When
            omitted, the existing params are preserved.
        headers : Headers | None | object, optional
            Replacement headers mapping. ``None`` clears headers. When
            omitted, the existing headers are preserved.
        timeout : float | None | object, optional
            Replacement timeout. ``None`` clears the timeout. When
            omitted, the existing timeout is preserved.

        Returns
        -------
        RequestOptions
            New snapshot reflecting the provided overrides.
        """
        if params is _UNSET:
            next_params = self.params
        elif params is None:
            next_params = None
        else:
            next_params = cast(dict, params)

        if headers is _UNSET:
            next_headers = self.headers
        elif headers is None:
            next_headers = None
        else:
            next_headers = cast(dict, headers)

        if timeout is _UNSET:
            next_timeout = self.timeout
        else:
            next_timeout = cast(float | None, timeout)

        return RequestOptions(
            params=next_params,
            headers=next_headers,
            timeout=next_timeout,
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
