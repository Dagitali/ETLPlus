"""
etlplus.config.api
===================

A module defining configuration types for REST APIs endpoint services.
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

from .pagination import PaginationConfig
from .rate_limit import RateLimitConfig
from .utils import pagination_from_defaults
from .utils import rate_limit_from_defaults

if TYPE_CHECKING:
    from .types import EndpointConfigMap, ApiConfigMap


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
        Default headers for the API.
    base_path : str | None
        Optional base path to prepend to endpoint paths.
    auth : dict[str, Any]
        Optional auth block (provider-specific shape, passed through).
    """

    # -- Attributes -- #

    base_url: str
    headers: dict[str, str] = field(default_factory=dict)
    base_path: str | None = None
    auth: dict[str, Any] = field(default_factory=dict)

    # Optional defaults carried at profile level
    pagination_defaults: PaginationConfig | None = None
    rate_limit_defaults: RateLimitConfig | None = None


@dataclass(slots=True)
class ApiConfig:
    """
    Configuration for a REST API service.

    Attributes
    ----------
    base_url : str
        Base URL for the API (may be derived from a selected profile). This
        field is always required and must be a non-empty string.
    headers : dict[str, str]
        Default headers for the API (may be derived from a selected profile).
    endpoints : dict[str, EndpointConfig]
        Configured endpoints for the API.
    profiles : dict[str, ApiProfileConfig]
        Optional named profiles providing per-environment base_url/headers.
    """

    # -- Attributes -- #

    base_url: str
    headers: dict[str, str] = field(default_factory=dict)
    endpoints: dict[str, EndpointConfig] = field(default_factory=dict)
    profiles: dict[str, ApiProfileConfig] = field(default_factory=dict)

    # -- Protected Instance Methods -- #

    def _selected_profile(self) -> ApiProfileConfig | None:
        """
        Return the active profile object, if any.

        Selection order mirrors headers/base_url behavior: 'default' when
        present, otherwise the first profile listed.

        Returns
        -------
        ApiProfileConfig | None
            The selected profile configuration, or None if no profiles exist.
        """

        name = self._selected_profile_name()
        return self.profiles.get(name) if name else None

    def _selected_profile_name(self) -> str | None:
        """
        Return the name of the selected profile, if any.

        Selection order mirrors headers/base_url behavior: 'default' when
        present, otherwise the first profile listed.

        Returns
        -------
        str | None
            The name of the selected profile, or None if no profiles exist.
        """

        if not self.profiles:
            return None

        return (
            'default' if 'default' in self.profiles
            else next(iter(self.profiles.keys()))
        )

    # -- Instance Methods -- #

    def build_endpoint_url(
        self,
        endpoint: EndpointConfig,
    ) -> str:
        """
        Compose a full URL from base_url, base_path, and endpoint.path.

        This mirrors EndpointClient.url path-join semantics, but uses
        the model's effective_base_url() which includes profile base_path.

        Parameters
        ----------
        endpoint : EndpointConfig
            The endpoint configuration.

        Returns
        -------
        str
            The full URL for the endpoint.
        """

        base = self.effective_base_url()
        parts = urlsplit(base)
        base_path = parts.path.rstrip('/')
        rel_norm = '/' + endpoint.path.lstrip('/')
        path = (base_path + rel_norm) if base_path else rel_norm

        return urlunsplit(
            (parts.scheme, parts.netloc, path, parts.query, parts.fragment),
        )

    def effective_base_path(self) -> str | None:
        """
        Return the selected profile's base_path, if any.

        Returns
        -------
        str | None
            The base path from the selected profile, or None if not set.
        """

        prof = self._selected_profile()

        return getattr(prof, 'base_path', None) if prof else None

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
        Get the effective pagination defaults for the API.

        Returns
        -------
        PaginationConfig | None
            The pagination defaults from the selected profile, or None if not
            set.
        """

        prof = self._selected_profile()

        return getattr(prof, 'pagination_defaults', None) if prof else None

    def effective_rate_limit_defaults(self) -> RateLimitConfig | None:
        """
        Get the effective rate limit defaults for the API.

        Returns
        -------
        RateLimitConfig | None
            The rate limit defaults from the selected profile, or None if not
            set.
        """

        prof = self._selected_profile()

        return getattr(prof, 'rate_limit_defaults', None) if prof else None

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
        """
        Create an ApiConfig instance from a dictionary-like object.

        Parameters
        ----------
        obj : Config
            The object to parse (expected to be a mapping).

        Returns
        -------
        ApiConfig
            The parsed ApiConfig instance.

        Notes
        -----
        TypedDict shape: ApiConfigMap (for editor and type-checkers).
        """

        # Accept any mapping-like object; provide a consistent error otherwise.
        if not isinstance(obj, Mapping):
            raise TypeError('ApiConfig must be a mapping')

        # Optional: profiles structure
        profiles_raw = obj.get('profiles', {}) or {}
        profiles: dict[str, ApiProfileConfig] = {}
        if isinstance(profiles_raw, dict):
            def _merge_headers(
                defaults_raw: Mapping[str, Any] | None,
                headers_raw: Mapping[str, Any] | None,
            ) -> dict[str, str]:
                dflt = {k: str(v) for k, v in (defaults_raw or {}).items()}
                hdrs = {k: str(v) for k, v in (headers_raw or {}).items()}
                return dflt | hdrs

            for name, p in profiles_raw.items():
                if isinstance(p, dict):
                    if not isinstance((p_base := p.get('base_url')), str):
                        raise TypeError(
                            'ApiProfileConfig requires "base_url" (str)',
                        )

                    # Merge defaults.headers (low precedence) over profile
                    # headers (high precedence).
                    merged_headers = _merge_headers(
                        (p.get('defaults', {}) or {}).get('headers', {}),
                        p.get('headers', {}),
                    )
                    base_path = p.get('base_path')
                    auth = dict(p.get('auth', {}) or {})
                    # Optional defaults: pagination/rate_limit
                    defaults_raw = p.get('defaults', {}) or {}
                    pag_def = pagination_from_defaults(
                        defaults_raw.get('pagination'),
                    )
                    rl_def = rate_limit_from_defaults(
                        defaults_raw.get('rate_limit'),
                    )
                    profiles[str(name)] = ApiProfileConfig(
                        base_url=p_base,
                        headers=merged_headers,
                        base_path=base_path,
                        auth=auth,
                        pagination_defaults=pag_def,
                        rate_limit_defaults=rl_def,
                    )

        # Top-level fallbacks (or legacy flat shape).
        tl_base = obj.get('base_url')
        tl_headers = {
            k: str(v)
            for k, v in (obj.get('headers', {}) or {}).items()
        }

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
        eps: dict[str, EndpointConfig] = {}
        if isinstance(raw_eps, dict):
            eps = {
                str(name): EndpointConfig.from_obj(ep)
                for name, ep in raw_eps.items()
            }

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
    method : str, optional
        HTTP method for the endpoint (e.g., "GET", "POST"). Defaults to "GET".
    path_params : dict[str, Any], optional
        Path parameters for the endpoint. Defaults to an empty dictionary.
    query_params : dict[str, Any], optional
        Default query parameters for the endpoint. Defaults to an empty
        dictionary.
    body : Any | None, optional
        Request body configuration for the endpoint. Defaults to None.
    pagination : PaginationConfig | None, optional
        Pagination configuration for the endpoint. Defaults to None.
    rate_limit : RateLimitConfig | None, optional
        Rate limiting configuration for the endpoint. Defaults to None.
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
        """
        Create an EndpointConfig instance from a string or dictionary-like
        object.

        Parameters
        ----------
        obj : str | Config
            The object to parse (expected to be a str or mapping).

        Returns
        -------
        EndpointConfig
            The parsed EndpointConfig instance.

        Notes
        -----
        TypedDict shape: EndpointConfigMap (for editor and type-checkers).
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
