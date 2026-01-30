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
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from typing import Self
from typing import TypedDict
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
    # Typed Dicts
    'ApiConfigMap',
    'ApiProfileConfigMap',
    'ApiProfileDefaultsMap',
    'EndpointMap',
]


# SECTION: CONSTANTS ======================================================== #


_UNSET: object = object()


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _to_dict(
    value: Mapping[str, Any] | object | None,
) -> dict[str, Any] | None:
    """
    Return a defensive ``dict`` copy for mapping inputs.

    Parameters
    ----------
    value : Mapping[str, Any] | object | None
        Mapping to copy, or ``None``.

    Returns
    -------
    dict[str, Any] | None
        New ``dict`` instance or ``None`` when the input is ``None``.
    """
    if value is None:
        return None
    return cast(dict[str, Any], value)


# SECTION: TYPED DICTS ====================================================== #


class ApiConfigMap(TypedDict, total=False):
    """
    Top-level API config shape parsed by
    :meth:`etlplus.api.config.ApiConfig.from_obj`.

    Either provide a :attr:`base_url` with optional :attr:`headers` and
    :attr:`endpoints`, or provide :attr:`profiles` with at least one profile
    having a :attr:`base_url`.

    See Also
    --------
    - :class:`etlplus.api.config.ApiConfig`
    """

    base_url: str
    headers: StrAnyMap
    endpoints: Mapping[str, EndpointMap | str]
    profiles: Mapping[str, ApiProfileConfigMap]


class ApiProfileConfigMap(TypedDict, total=False):
    """
    Shape accepted for a profile entry under
    :meth:`etlplus.api.config.ApiConfig.from_obj`.

    Notes
    -----
    - :attr:`base_url` is required at runtime when :attr:`profiles` key/value
        pairs are provided.
    - :meth:`etlplus.api.config.ApiProfileConfig.from_obj` parses this mapping.

    See Also
    --------
    - :class:`etlplus.api.config.ApiProfileConfig`
    """

    base_url: str
    headers: StrAnyMap
    base_path: str
    auth: StrAnyMap
    defaults: ApiProfileDefaultsMap


class ApiProfileDefaultsMap(TypedDict, total=False):
    """
    Defaults block available under a profile (all keys optional).

    Notes
    -----
    - Runtime expects header values to be ``str``; typing remains permissive.
    - :meth:`etlplus.api.config.ApiProfileConfig.from_obj` consumes this block.
    - :meth:`etlplus.api.pagination.PaginationConfig.from_obj` parses
        :attr:`pagination`.
    - :meth:`etlplus.api.rate_limiting.RateLimitConfig.from_obj` parses
        :attr:`rate_limit`.

    See Also
    --------
    - :class:`etlplus.api.config.ApiProfileConfig`
    - :class:`etlplus.api.pagination.PaginationConfig`
    - :class:`etlplus.api.rate_limiting.RateLimitConfig`
    """

    headers: StrAnyMap
    pagination: StrAnyMap  # PaginationConfigMap | StrAnyMap
    rate_limit: StrAnyMap  # RateLimitConfigMap | StrAnyMap


class EndpointMap(TypedDict, total=False):
    """
    Shape accepted by :meth:`etlplus.api.config.EndpointConfig.from_obj`.

    One of :attr:`path` or :attr:`url` should be provided.

    See Also
    --------
    - :class:`etlplus.api.config.EndpointConfig`
    """

    path: str
    url: str
    method: str
    path_params: StrAnyMap
    query_params: StrAnyMap
    body: Any
    pagination: StrAnyMap  # PaginationConfigMap | StrAnyMap
    rate_limit: StrAnyMap  # RateLimitConfigMap | StrAnyMap


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
        if self.params is not None:
            object.__setattr__(self, 'params', _to_dict(self.params))
        if self.headers is not None:
            object.__setattr__(self, 'headers', _to_dict(self.headers))

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
        if self.params is not None:
            kw['params'] = dict(self.params)
        if self.headers is not None:
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
    ) -> Self:
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
        Self
            New snapshot reflecting the provided overrides.
        """
        if params is _UNSET:
            next_params = self.params
        else:
            # next_params = _to_dict(params) if params is not None else None
            next_params = _to_dict(params)

        if headers is _UNSET:
            next_headers = self.headers
        else:
            # next_headers = _to_dict(headers) if headers is not None else None
            next_headers = _to_dict(headers)
        if timeout is _UNSET:
            next_timeout = self.timeout
        else:
            next_timeout = cast(float | None, timeout)

        return self.__class__(
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
