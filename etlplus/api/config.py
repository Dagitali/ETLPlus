"""
:mod:`etlplus.api.config` module.

Configuration dataclasses for REST API services, profiles, and endpoints.

Notes
-----
- TypedDict references remain editor hints only; :meth:`from_obj` accepts
    ``StrAnyMap`` for permissive parsing.
- Helper functions near the bottom keep parsing logic centralized and avoid
    leaking implementation details.
"""

from __future__ import annotations

from collections.abc import Callable
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from types import MappingProxyType
from typing import TYPE_CHECKING
from typing import Any
from typing import Self
from typing import overload
from urllib.parse import urlsplit
from urllib.parse import urlunsplit

from ..types import StrAnyMap
from ..types import StrStrMap
from ..utils import cast_str_dict
from ..utils import coerce_dict
from ..utils import maybe_mapping
from .endpoint_client import EndpointClient
from .enums import HttpMethod
from .pagination import PaginationConfig
from .rate_limiting import RateLimitConfig

if TYPE_CHECKING:
    from .types import ApiConfigMap
    from .types import ApiProfileConfigMap
    from .types import EndpointMap


# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Data Classes
    'ApiConfig',
    'ApiProfileConfig',
    'EndpointConfig',
]


# SECTION: INTERNAL CONSTANTS =============================================== #


_HTTP_METHODS: tuple[str, ...] = tuple(member.name for member in HttpMethod)


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _effective_service_defaults(
    *,
    profiles: Mapping[str, ApiProfileConfig],
    fallback_base: Any,
    fallback_headers: dict[str, str],
) -> tuple[str, dict[str, str]]:
    """
    Return ``(base_url, headers)`` using *profiles* when present.

    Parameters
    ----------
    profiles : Mapping[str, ApiProfileConfig]
        Named profile configurations.
    fallback_base : Any
        Top-level base URL when no profiles are defined.
    fallback_headers : dict[str, str]
        Top-level headers when no profiles are defined.

    Returns
    -------
    tuple[str, dict[str, str]]
        Effective ``(base_url, headers)`` pair.

    Raises
    ------
    TypeError
        If no profiles are defined and *fallback_base* is not a string.
    """
    if profiles:
        name = 'default' if 'default' in profiles else next(iter(profiles))
        selected = profiles[name]
        headers = dict(selected.headers)
        if fallback_headers:
            headers |= fallback_headers
        return selected.base_url, headers

    if not isinstance(fallback_base, str):
        raise TypeError('ApiConfig requires "base_url" (str)')
    return fallback_base, fallback_headers


def _freeze_mapping(
    mapping: Mapping[Any, Any],
    *,
    key_cast: Callable[[Any], Any] | None = None,
) -> MappingProxyType:
    """
    Return an immutable copy of a mapping, optionally normalizing keys.

    Parameters
    ----------
    mapping : Mapping[Any, Any]
        Source mapping to freeze.
    key_cast : Callable[[Any], Any] | None, optional
        Optional key coercion applied to each key.

    Returns
    -------
    MappingProxyType
        Read-only mapping proxy with normalized keys.
    """
    if key_cast is None:
        data = dict(mapping)
    else:
        data = {key_cast(key): value for key, value in mapping.items()}
    return MappingProxyType(data)


def _normalize_method(
    value: Any,
) -> Any | None:
    """
    Return a validated HTTP method string or pass through custom inputs.

    Parameters
    ----------
    value : Any
        Raw method value.

    Returns
    -------
    Any | None
        Normalized method string, ``None``, or original input.

    Raises
    ------
    ValueError
        If the string value is not a supported HTTP method.
    """
    if value is None:
        return None
    if isinstance(value, HttpMethod):
        return value.name
    if isinstance(value, str):
        normalized = value.strip().upper()
        if not normalized:
            return None
        if normalized not in _HTTP_METHODS:
            raise ValueError(
                f'Unsupported HTTP method {normalized!r}; '
                f'must be one of {_HTTP_METHODS}',
            )
        return normalized
    return value


def _parse_endpoints(
    raw: Any,
) -> dict[str, EndpointConfig]:
    """
    Return parsed endpoint configs keyed by name.

    Parameters
    ----------
    raw : Any
        Raw endpoint mapping.

    Returns
    -------
    dict[str, EndpointConfig]
        Parsed endpoint configurations.
    """
    if not (mapping := maybe_mapping(raw)):
        return {}
    return {
        str(name): EndpointConfig.from_obj(data)
        for name, data in mapping.items()
    }


def _parse_profiles(raw: Any) -> dict[str, ApiProfileConfig]:
    """
    Return parsed API profiles keyed by name.

    Parameters
    ----------
    raw : Any
        Raw profiles mapping.

    Returns
    -------
    dict[str, ApiProfileConfig]
            Parsed API profile configurations.
    """
    if not (mapping := maybe_mapping(raw)):
        return {}
    parsed: dict[str, ApiProfileConfig] = {}
    for name, profile_raw in mapping.items():
        if not (profile_map := maybe_mapping(profile_raw)):
            continue
        parsed[str(name)] = ApiProfileConfig.from_obj(profile_map)
    return parsed


# SECTION: DATA CLASSES ===================================================== #


@dataclass(slots=True, kw_only=True)
class ApiProfileConfig:
    """
    Profile configuration for a REST API service.

    Attributes
    ----------
    base_url : str
        Base URL for the API.
    headers : StrStrMap
        Profile-level default headers (merged with defaults.headers).
    base_path : str | None
        Optional base path prefixed to endpoint paths when composing URLs.
    auth : StrAnyMap
        Optional auth block (provider-specific shape, passed through).
    pagination_defaults : PaginationConfig | None
        Optional pagination defaults applied to endpoints referencing this
        profile (lowest precedence).
    rate_limit_defaults : RateLimitConfig | None
        Optional rate limit defaults applied to endpoints referencing this
        profile (lowest precedence).
    """

    # -- Attributes -- #

    base_url: str
    headers: StrStrMap = field(default_factory=dict)
    base_path: str | None = None
    auth: StrAnyMap = field(default_factory=dict)

    # Optional defaults carried at profile level
    pagination_defaults: PaginationConfig | None = None
    rate_limit_defaults: RateLimitConfig | None = None

    # -- Magic Methods (Object Lifecycle) -- #

    def __post_init__(self) -> None:
        object.__setattr__(self, 'headers', _freeze_mapping(self.headers))
        object.__setattr__(self, 'auth', _freeze_mapping(self.auth))

    # -- Class Methods -- #

    @classmethod
    @overload
    def from_obj(
        cls,
        obj: ApiProfileConfigMap,
    ) -> Self: ...

    @classmethod
    @overload
    def from_obj(
        cls,
        obj: StrAnyMap,
    ) -> Self: ...

    @classmethod
    def from_obj(
        cls,
        obj: StrAnyMap,
    ) -> Self:
        """
        Parse a mapping into an :class:`ApiProfileConfig` instance.

        Parameters
        ----------
        obj : StrAnyMap
            Raw profile configuration.

        Returns
        -------
        Self
            Parsed profile configuration.

        Raises
        ------
        TypeError
            If required fields are missing or of incorrect type.
        """
        if not isinstance(obj, Mapping):
            raise TypeError('ApiProfileConfig must be a mapping')

        if not isinstance((base := obj.get('base_url')), str):
            raise TypeError('ApiProfileConfig requires "base_url" (str)')

        defaults_raw = coerce_dict(obj.get('defaults'))
        merged_headers = cast_str_dict(
            defaults_raw.get('headers'),
        ) | cast_str_dict(obj.get('headers'))

        base_path = obj.get('base_path')
        auth = coerce_dict(obj.get('auth'))

        pag_def = PaginationConfig.from_defaults(
            defaults_raw.get('pagination'),
        )
        rl_def = RateLimitConfig.from_defaults(defaults_raw.get('rate_limit'))

        return cls(
            base_url=base,
            headers=merged_headers,
            base_path=base_path,
            auth=auth,
            pagination_defaults=pag_def,
            rate_limit_defaults=rl_def,
        )


@dataclass(slots=True, kw_only=True)
class ApiConfig:
    """
    Configuration for a REST API service.

    Attributes
    ----------
    base_url : str
        Effective base URL (derived from profiles or top-level input).
    headers : StrStrMap
        Effective headers (profile + top-level merged with precedence).
    endpoints : Mapping[str, EndpointConfig]
        Endpoint configurations keyed by name.
    profiles : Mapping[str, ApiProfileConfig]
        Named profile configurations; first or ``default`` becomes active.
    """

    # -- Attributes -- #

    base_url: str
    headers: StrStrMap = field(default_factory=dict)
    endpoints: Mapping[str, EndpointConfig] = field(default_factory=dict)

    # See also: ApiProfileConfig.from_obj for profile parsing logic.
    profiles: Mapping[str, ApiProfileConfig] = field(default_factory=dict)

    # -- Magic Methods (Object Lifecycle) -- #

    def __post_init__(self) -> None:
        object.__setattr__(self, 'headers', _freeze_mapping(self.headers))
        object.__setattr__(
            self,
            'endpoints',
            _freeze_mapping(self.endpoints, key_cast=str),
        )
        object.__setattr__(
            self,
            'profiles',
            _freeze_mapping(self.profiles, key_cast=str),
        )

    # -- Internal Instance Methods -- #

    def _selected_profile(self) -> ApiProfileConfig | None:
        """
        Return the active profile object (``default`` preferred) or ``None``.
        """
        if not (profiles := self.profiles):
            return None

        name = 'default' if 'default' in profiles else next(iter(profiles))

        return profiles.get(name)

    def _profile_attr(
        self,
        attr: str,
    ) -> Any:
        """
        Return an attribute on the selected profile, if available.

        Parameters
        ----------
        attr : str
            Attribute name to retrieve.

        Returns
        -------
        Any
            Attribute value or ``None`` if no profile is selected.
        """
        prof = self._selected_profile()

        return getattr(prof, attr, None) if prof else None

    # -- Instance Methods -- #

    def build_endpoint_url(
        self,
        endpoint: EndpointConfig,
    ) -> str:
        """
        Compose a full URL from ``base_url``, ``base_path``, and endpoint path.

        Parameters
        ----------
        endpoint : EndpointConfig
            Endpoint configuration.

        Returns
        -------
        str
            Full endpoint URL.
        """
        client = EndpointClient(
            base_url=self.base_url,
            base_path=self.effective_base_path(),
            endpoints={'__ep__': endpoint.path},
        )

        return client.url('__ep__')

    def effective_base_path(self) -> str | None:
        """Return the selected profile's ``base_path``, if any."""
        return self._profile_attr('base_path')

    def effective_base_url(self) -> str:
        """
        Compute ``base_url`` combined with effective ``base_path`` when set.
        """
        parts = urlsplit(self.base_url)
        base_path = parts.path.rstrip('/')
        extra = self.effective_base_path()
        extra_norm = ('/' + extra.lstrip('/')) if extra else ''
        path = (base_path + extra_norm) if (base_path or extra_norm) else ''

        return urlunsplit(
            (parts.scheme, parts.netloc, path, parts.query, parts.fragment),
        )

    def effective_pagination_defaults(self) -> PaginationConfig | None:
        """Return selected profile ``pagination_defaults``, if any."""
        return self._profile_attr('pagination_defaults')

    def effective_rate_limit_defaults(self) -> RateLimitConfig | None:
        """Return selected profile ``rate_limit_defaults``, if any."""
        return self._profile_attr('rate_limit_defaults')

    # -- Class Methods -- #

    @classmethod
    @overload
    def from_obj(
        cls,
        obj: ApiConfigMap,
    ) -> Self: ...

    @classmethod
    @overload
    def from_obj(
        cls,
        obj: StrAnyMap,
    ) -> Self: ...

    @classmethod
    def from_obj(
        cls,
        obj: StrAnyMap,
    ) -> Self:
        """
        Parse a mapping into an :class:`ApiConfig` instance.

        Parameters
        ----------
        obj : StrAnyMap
            Raw API configuration.

        Returns
        -------
        Self
            Parsed API configuration.

        Raises
        ------
        TypeError
            If required fields are missing or of incorrect type.
        """
        if not isinstance(obj, Mapping):
            raise TypeError('ApiConfig must be a mapping')

        profiles = _parse_profiles(obj.get('profiles'))

        tl_base = obj.get('base_url')
        tl_headers = cast_str_dict(obj.get('headers'))

        base_url, headers = _effective_service_defaults(
            profiles=profiles,
            fallback_base=tl_base,
            fallback_headers=tl_headers,
        )

        endpoints = _parse_endpoints(obj.get('endpoints'))

        return cls(
            base_url=base_url,
            headers=headers,
            endpoints=endpoints,
            profiles=profiles,
        )


@dataclass(slots=True, kw_only=True)
class EndpointConfig:
    """
    Configuration for a single API endpoint.

    Attributes
    ----------
    path : str
        Endpoint path (relative to base URL).
    method : str | None
        Optional HTTP method (default is GET when omitted at runtime).
    path_params : StrAnyMap
        Path parameters used when constructing the request URL.
    query_params : StrAnyMap
        Default query string parameters.
    body : Any | None
        Request body structure (pass-through, format-specific).
    pagination : PaginationConfig | None
        Pagination configuration for the endpoint.
    rate_limit : RateLimitConfig | None
        Rate limit configuration for the endpoint.
    """

    # -- Attributes -- #

    path: str
    method: str | None = None
    path_params: StrAnyMap = field(default_factory=dict)
    query_params: StrAnyMap = field(default_factory=dict)
    body: Any | None = None
    pagination: PaginationConfig | None = None
    rate_limit: RateLimitConfig | None = None

    # -- Magic Methods (Object Lifecycle) -- #

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            'path_params',
            _freeze_mapping(self.path_params),
        )
        object.__setattr__(
            self,
            'query_params',
            _freeze_mapping(self.query_params),
        )

    # -- Class Methods -- #

    @classmethod
    @overload
    def from_obj(
        cls,
        obj: str,
    ) -> Self: ...

    @classmethod
    @overload
    def from_obj(
        cls,
        obj: EndpointMap,
    ) -> Self: ...

    @classmethod
    def from_obj(
        cls,
        obj: str | StrAnyMap,
    ) -> Self:
        """
        Parse a string or mapping into an :class:`EndpointConfig` instance.

        Parameters
        ----------
        obj : str | StrAnyMap
            Raw endpoint configuration.

        Returns
        -------
        Self
            Parsed endpoint configuration.

        Raises
        ------
        TypeError
            If required fields are missing or of incorrect type.
        ValueError
            If provided method is not a supported HTTP method.
        """
        match obj:
            case str():
                return cls(path=obj, method=None)
            case Mapping():
                path = obj.get('path') or obj.get('url')
                if not isinstance(path, str):
                    raise TypeError('EndpointConfig requires a "path" (str)')

                path_params_raw = obj.get('path_params')
                if path_params_raw is not None and not isinstance(
                    path_params_raw,
                    Mapping,
                ):
                    raise ValueError('path_params must be a mapping if set')

                query_params_raw = obj.get('query_params')
                if query_params_raw is not None and not isinstance(
                    query_params_raw,
                    Mapping,
                ):
                    raise TypeError('query_params must be a mapping if set')

                return cls(
                    path=path,
                    method=_normalize_method(obj.get('method')),
                    path_params=coerce_dict(path_params_raw),
                    query_params=coerce_dict(query_params_raw),
                    body=obj.get('body'),
                    pagination=PaginationConfig.from_obj(
                        obj.get('pagination'),
                    ),
                    rate_limit=RateLimitConfig.from_obj(obj.get('rate_limit')),
                )
            case _:
                raise TypeError(
                    'Invalid endpoint config: expected str or mapping',
                )
