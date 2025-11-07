"""
etlplus.config.api

Configuration models for REST API services and endpoints.

Notes
-----
- TypedDict shapes are editor hints; runtime parsing remains permissive
    (``from_obj`` accepts ``Mapping[str, Any]``).
- Profile-level defaults (pagination and rate limit) are normalized at parse
    time and exposed via convenience accessors.

See Also
--------
- :class:`ApiProfileConfig` — canonical parsing for profiles (used when
    processing the ``profiles`` section).
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import overload
from typing import Self
from typing import TYPE_CHECKING
from urllib.parse import urlsplit
from urllib.parse import urlunsplit

from ..api import EndpointClient
from .pagination import PaginationConfig
from .rate_limit import RateLimitConfig
from .utils import cast_str_dict
from .utils import pagination_from_defaults
from .utils import rate_limit_from_defaults

if TYPE_CHECKING:
    from .types import ApiConfigMap, ApiProfileConfigMap, EndpointConfigMap


# SECTION: EXPORTS ========================================================== #


__all__ = [
    'ApiConfig',
    'ApiProfileConfig',
    'EndpointConfig',
]


# SECTION: CLASSES ========================================================== #


@dataclass(slots=True)
class ApiProfileConfig:
    """
    Profile configuration for a REST API service.

    Attributes
    ----------
    base_url : str
        Base URL for the API.
    headers : dict[str, str]
        Profile-level default headers (merged with defaults.headers).
    base_path : str | None
        Optional base path prefixed to endpoint paths when composing URLs.
    auth : dict[str, Any]
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
    headers: dict[str, str] = field(default_factory=dict)
    base_path: str | None = None
    auth: dict[str, Any] = field(default_factory=dict)

    # Optional defaults carried at profile level
    pagination_defaults: PaginationConfig | None = None
    rate_limit_defaults: RateLimitConfig | None = None

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
        obj: Mapping[str, Any],
    ) -> Self: ...

    @classmethod
    def from_obj(
        cls,
        obj: Mapping[str, Any],
    ) -> Self:
        """
        Parse a mapping into an ``ApiProfileConfig`` instance.

        Parameters
        ----------
        obj : Mapping[str, Any]
            Mapping with at least ``base_url``.

        Returns
        -------
        ApiProfileConfig
            Parsed profile configuration.

        Raises
        ------
        TypeError
            If ``obj`` is not a mapping or ``base_url`` is missing/invalid.

        Notes
        -----
        TypedDict shape: ``ApiProfileConfigMap`` (editor/type-checker hint).
        """
        if not isinstance(obj, Mapping):
            raise TypeError('ApiProfileConfig must be a mapping')

        if not isinstance((base := obj.get('base_url')), str):
            raise TypeError('ApiProfileConfig requires "base_url" (str)')

        defaults_raw = obj.get('defaults', {}) or {}
        merged_headers = (
            cast_str_dict(defaults_raw.get('headers'))
            | cast_str_dict(obj.get('headers'))
        )

        base_path = obj.get('base_path')
        auth = dict(obj.get('auth', {}) or {})

        pag_def = pagination_from_defaults(defaults_raw.get('pagination'))
        rl_def = rate_limit_from_defaults(defaults_raw.get('rate_limit'))

        return cls(
            base_url=base,
            headers=merged_headers,
            base_path=base_path,
            auth=auth,
            pagination_defaults=pag_def,
            rate_limit_defaults=rl_def,
        )


@dataclass(slots=True)
class ApiConfig:
    """
    Configuration for a REST API service.

    Attributes
    ----------
    base_url : str
        Effective base URL (derived from profiles or top-level input).
    headers : dict[str, str]
        Effective headers (profile + top-level merged with precedence).
    endpoints : dict[str, EndpointConfig]
        Endpoint configurations keyed by name.
    profiles : dict[str, ApiProfileConfig]
        Named profile configurations; first or ``default`` becomes active.
    """

    # -- Attributes -- #

    base_url: str
    headers: dict[str, str] = field(default_factory=dict)
    endpoints: dict[str, EndpointConfig] = field(default_factory=dict)

    # See also: ApiProfileConfig.from_obj for profile parsing logic.
    profiles: dict[str, ApiProfileConfig] = field(default_factory=dict)

    # -- Protected Instance Methods -- #

    def _selected_profile(self) -> ApiProfileConfig | None:
        """
        Return the active profile object ("default" preferred) or None.

        Uses a tiny helper for selection and avoids duplicating logic.

        Returns
        -------
        ApiProfileConfig | None
            The selected profile configuration, or None if no profiles exist.
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
        Generic accessor for an attribute on the selected profile.

        This centralizes profile selection logic so "effective_*" helpers
        become one-liners. Returns None if no profile or attribute missing.

        Parameters
        ----------
        attr : str
            Attribute name to fetch from the selected profile.

        Returns
        -------
        Any
            The attribute value, or ``None`` when unavailable.
        """
        prof = self._selected_profile()

        return getattr(prof, attr, None) if prof else None

    # -- Instance Methods -- #

    def build_endpoint_url(
        self,
        endpoint: EndpointConfig,
    ) -> str:
        """
        Compose a full URL from base_url, base_path, and endpoint.path.

        Implementation delegates URL joining to EndpointClient so that
        path composition stays consistent with the client (including
        handling of leading/trailing slashes and optional base_path).

        Parameters
        ----------
        endpoint : EndpointConfig
            The endpoint configuration.

        Returns
        -------
        str
            The full URL for the endpoint.
        """
        client = EndpointClient(
            base_url=self.base_url,
            base_path=self.effective_base_path(),
            endpoints={'__ep__': endpoint.path},
        )

        return client.url('__ep__')

    def effective_base_path(self) -> str | None:
        """
        Return the selected profile's base_path, if any.

        Returns
        -------
        str | None
            The base path from the selected profile, or None if not set.
        """
        return self._profile_attr('base_path')

    def effective_base_url(self) -> str:
        """
        Compute base_url combined with effective base_path, if present.

        Returns
        -------
        str
            The effective base URL including base_path.
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
        """
        Return selected profile pagination_defaults, if any.

        Returns
        -------
        PaginationConfig | None
            The pagination defaults from the selected profile, or None if not
            set.
        """
        return self._profile_attr('pagination_defaults')

    def effective_rate_limit_defaults(self) -> RateLimitConfig | None:
        """
        Return selected profile rate_limit_defaults, if any.

        Returns
        -------
        RateLimitConfig | None
            The rate limit defaults from the selected profile, or None if not
            set.
        """
        return self._profile_attr('rate_limit_defaults')

    # -- Static Methods -- #

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
        obj: Mapping[str, Any],
    ) -> Self: ...

    @classmethod
    def from_obj(
        cls,
        obj: Mapping[str, Any],
    ) -> Self:
        """Parse a mapping into an ``ApiConfig`` instance.

        Parameters
        ----------
        obj : Mapping[str, Any]
            Mapping containing either ``base_url`` or a ``profiles`` block.

        Returns
        -------
        ApiConfig
            Parsed API configuration.

        Raises
        ------
        TypeError
            If ``obj`` is not a mapping or mandatory keys are missing.

        Notes
        -----
        TypedDict shape: ``ApiConfigMap`` (editor/type-checker hint).
        """
        # Accept any mapping-like object; provide a consistent error otherwise.
        if not isinstance(obj, Mapping):
            raise TypeError('ApiConfig must be a mapping')

        # Optional: profiles structure
        # See also: ApiProfileConfig.from_obj for profile parsing logic.
        profiles_raw = obj.get('profiles', {}) or {}
        profiles: dict[str, ApiProfileConfig] = (
            {
                str(name): ApiProfileConfig.from_obj(p)
                for name, p in profiles_raw.items()
                if isinstance(p, dict)
            }
            if isinstance(profiles_raw, dict) else {}
        )

        # Top-level fallbacks (or legacy flat shape).
        tl_base = obj.get('base_url')
        tl_headers = cast_str_dict(obj.get('headers'))

        # Determine effective base_url/headers for backward compatibility
        # Always compute a concrete str for base_url.
        base_url: str
        headers: dict[str, str] = {}
        if profiles:
            # Choose a default profile: explicit 'default' else first key
            prof_name = 'default' if 'default' in profiles else (
                next(iter(profiles.keys()))
            )
            base_url = profiles[prof_name].base_url
            headers = dict(profiles[prof_name].headers)
            # Merge in top-level headers as overrides if provided
            if tl_headers:
                headers |= tl_headers
        else:
            # Legacy flat shape must provide base_url.
            if not isinstance(tl_base, str):
                raise TypeError('ApiConfig requires "base_url" (str)')
            base_url = tl_base
            headers = tl_headers
        raw_eps = obj.get('endpoints', {}) or {}
        eps: dict[str, EndpointConfig] = (
            {
                str(name): EndpointConfig.from_obj(ep)
                for name, ep in raw_eps.items()
            }
            if isinstance(raw_eps, dict) else {}
        )

        return cls(
            base_url=base_url,
            headers=headers,
            endpoints=eps,
            profiles=profiles,
        )


@dataclass(slots=True)
class EndpointConfig:
    """
    Configuration for a single API endpoint.

    Attributes
    ----------
    path : str
        Endpoint path (relative to base URL).
    method : str | None
        Optional HTTP method (default is GET when omitted at runtime).
    path_params : dict[str, Any]
        Path parameters used when constructing the request URL.
    query_params : dict[str, Any]
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
    path_params: dict[str, Any] = field(default_factory=dict)
    query_params: dict[str, Any] = field(default_factory=dict)
    body: Any | None = None
    pagination: PaginationConfig | None = None
    rate_limit: RateLimitConfig | None = None

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
        obj: EndpointConfigMap,
    ) -> Self: ...

    @classmethod
    def from_obj(
        cls,
        obj: str | Mapping[str, Any],
    ) -> Self:
        """Parse a string or mapping into an ``EndpointConfig`` instance.

        Parameters
        ----------
        obj : str | Mapping[str, Any]
            Either a bare path string or a mapping with endpoint fields.

        Returns
        -------
        EndpointConfig
            Parsed endpoint configuration.

        Raises
        ------
        TypeError
            If the input is neither str nor mapping, or ``path`` is missing.

        Notes
        -----
        TypedDict shape: ``EndpointConfigMap`` (editor/type-checker hint).
        """
        # Allow either a bare string path or a mapping with explicit fields.
        if isinstance(obj, str):
            return cls(path=obj, method=None)
        if isinstance(obj, Mapping):
            path = obj.get('path') or obj.get('url')
            if not isinstance(path, str):
                raise TypeError('EndpointConfig requires a "path" (str)')

            return cls(
                path=path,
                method=obj.get('method'),
                path_params=dict(obj.get('path_params', {}) or {}),
                query_params=dict(obj.get('query_params', {}) or {}),
                body=obj.get('body'),
                pagination=PaginationConfig.from_obj(obj.get('pagination')),
                rate_limit=RateLimitConfig.from_obj(obj.get('rate_limit')),
            )

        raise TypeError('Invalid endpoint config: must be str or mapping')
