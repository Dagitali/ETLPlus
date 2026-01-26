"""
:mod:`etlplus.api.endpoint_client` module.

Endpoint client for composing URLs, requests, and pagination.

This module provides :class:`EndpointClient`, a small frozen dataclass that
registers endpoint paths under a base URL, applies retry and rate-limiting
policies, and wires pagination helpers to fetch JSON records from REST APIs.

Notes
-----
- Retry-related types live in :mod:`etlplus.api.retry_manager`.
- Pagination requires a ``PaginationConfig``; see
    :class:`PagePaginationConfigMap` and :class:`CursorPaginationConfigMap` for
    the accepted shapes.

Examples
--------
>>> # Page-based pagination
>>> client = EndpointClient(
...     base_url="https://api.example.com/v1",
...     endpoints={"list": "/items"},
... )
>>> pg = {"type": "page", "page_size": 100}
>>> rows = client.paginate("list", pagination=pg)

>>> # Cursor-based pagination
>>> pg = {
...     "type": "cursor",
...     "records_path": "data.items",
...     "cursor_param": "cursor",
...     "cursor_path": "data.nextCursor",
...     "page_size": 100,
... }
>>> rows = client.paginate("list", pagination=pg)
"""

from __future__ import annotations

import time
from collections.abc import Callable
from collections.abc import Iterator
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import field
from types import MappingProxyType
from types import TracebackType
from typing import Any
from typing import ClassVar
from typing import Self
from typing import cast
from urllib.parse import parse_qsl
from urllib.parse import quote
from urllib.parse import urlencode
from urllib.parse import urlsplit
from urllib.parse import urlunsplit

import requests  # type: ignore[import]

from ..types import JSONData
from ..types import JSONDict
from .errors import ApiRequestError
from .errors import PaginationError
from .pagination import PaginationClient
from .pagination import PaginationInput
from .pagination import Paginator
from .rate_limiting import RateLimitConfigMap
from .rate_limiting import RateLimiter
from .rate_limiting import RateLimitOverrides
from .request_manager import RequestManager
from .retry_manager import RetryManager
from .retry_manager import RetryPolicy
from .retry_manager import RetryStrategy
from .transport import HTTPAdapterMountConfig
from .types import RequestOptions
from .types import Url

# SECTION: CLASSES ========================================================== #


@dataclass(frozen=True, slots=True)
class EndpointClient:
    """
    Immutable registry of endpoint path templates rooted at a base URL.

    Summary
    -------
    Provides helpers for composing absolute URLs, paginating responses,
    applying client-wide rate limits, and performing jittered exponential
    backoff retries. The dataclass is frozen and uses ``slots`` for memory
    efficiency; mutating attribute values is disallowed.

    Parameters
    ----------
    base_url : Url
        Absolute base URL, e.g., ``"https://api.example.com/v1"``.
    endpoints : Mapping[str, str]
        Mapping of endpoint keys to relative paths, e.g.,
        ``{"list_users": "/users", "user": "/users/{id}"}``.
    base_path : str | None, optional
        Optional base path prefix (``/v2``) prepended to all endpoint
        paths when building URLs.
    retry : RetryPolicy | None, optional
        Optional retry policy. When provided, failed requests matching
        ``retry_on`` statuses are retried with full jitter.
    retry_network_errors : bool, optional
        When ``True``, also retry on network errors (timeouts, connection
        resets). Defaults to ``False``.
    rate_limit : RateLimitConfigMap | None, optional
        Optional client-wide rate limit used to derive an inter-request
        delay when an explicit ``sleep_seconds`` isn't supplied.
    session : requests.Session | None, optional
        Explicit HTTP session for all requests.
    session_factory : Callable[[], requests.Session] | None, optional
        Factory used to lazily create a session. Ignored if ``session`` is
        provided.
    session_adapters : Sequence[HTTPAdapterMountConfig] | None, optional
        Adapter mount configuration(s) used to build a session lazily when
        neither ``session`` nor ``session_factory`` is supplied.

    Attributes
    ----------
    base_url : Url
        Absolute base URL.
    endpoints : Mapping[str, str]
        Read-only mapping of endpoint keys to relative paths
        (``MappingProxyType``).
    base_path : str | None
        Optional base path prefix appended after ``base_url``.
    retry : RetryPolicy | None
        Retry policy reference (may be ``None``).
    retry_network_errors : bool
        Whether network errors are retried in addition to HTTP statuses.
    rate_limit : RateLimitConfigMap | None
        Client-wide rate limit configuration (may be ``None``).
    session : requests.Session | None
        Explicit HTTP session used for requests when provided.
    session_factory : Callable[[], requests.Session] | None
        Lazily invoked factory producing a session when needed.
    session_adapters : Sequence[HTTPAdapterMountConfig] | None
        Adapter mount configuration(s) for connection pooling / retries.
    DEFAULT_PAGE_PARAM : ClassVar[str]
        Default page parameter name.
    DEFAULT_SIZE_PARAM : ClassVar[str]
        Default page-size parameter name.
    DEFAULT_START_PAGE : ClassVar[int]
        Default starting page number.
    DEFAULT_PAGE_SIZE : ClassVar[int]
        Default records-per-page when unspecified.
    DEFAULT_CURSOR_PARAM : ClassVar[str]
        Default cursor parameter name.
    DEFAULT_LIMIT_PARAM : ClassVar[str]
        Default limit parameter name used for cursor pagination.
    DEFAULT_RETRY_MAX_ATTEMPTS : ClassVar[int]
        Fallback max attempts when retry policy omits it.
    DEFAULT_RETRY_BACKOFF : ClassVar[float]
        Fallback exponential backoff base seconds.
    DEFAULT_RETRY_ON : ClassVar[tuple[int, ...]]
        Default HTTP status codes eligible for retry.
    DEFAULT_RETRY_CAP : ClassVar[float]
        Maximum sleep seconds for jittered backoff.
    DEFAULT_TIMEOUT : ClassVar[float]
        Default timeout applied to HTTP requests when unspecified.

    Raises
    ------
    ValueError
        If ``base_url`` is not absolute or endpoint keys/values are invalid.

    Notes
    -----
    - Endpoint mapping is defensively copied and wrapped read-only.
    - Pagination defaults (page size, start page, cursor param, etc.) are
        centralized as class variables.
    - Context manager support (``with EndpointClient(...) as client``)
        manages session lifecycle; owned sessions are closed on exit.
    - Retries use exponential backoff with jitter capped by
        ``DEFAULT_RETRY_CAP`` seconds.

    Examples
    --------
    Basic URL composition
    ^^^^^^^^^^^^^^^^^^^^^
    >>> client = EndpointClient(
    ...     base_url="https://api.example.com/v1",
    ...     endpoints={"list_users": "/users", "user": "/users/{id}"},
    ... )
    >>> client.url("list_users", query_parameters={"active": "true"})
    'https://api.example.com/v1/users?active=true'
    >>> client.url("user", path_parameters={"id": 42})
    'https://api.example.com/v1/users/42'

    Page pagination with retries
    ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    >>> client = EndpointClient(
    ...     base_url="https://api.example.com/v1",
    ...     endpoints={"list": "/items"},
    ...     retry={"max_attempts": 5, "backoff": 0.5, "retry_on": [429, 503]},
    ...     retry_network_errors=True,
    ... )
    >>> rows = client.paginate(
    ...     "list",
    ...     pagination={"type": "page", "page_size": 50},
    ... )
    """

    # -- Attributes -- #

    base_url: Url
    endpoints: Mapping[str, str]
    base_path: str | None = None

    # Optional retry configuration (constructor parameter; object is frozen)
    retry: RetryPolicy | None = None
    retry_network_errors: bool = False
    # Optional client-wide rate limit configuration
    rate_limit: RateLimitConfigMap | None = None

    # Optional HTTP session or factory
    session: requests.Session | None = None
    session_factory: Callable[[], requests.Session] | None = None

    # Optional HTTPAdapter mount configuration(s) for transport-level retries
    # and connection pooling. If provided and neither `session` nor
    # `session_factory` is supplied, a factory is synthesized to create a
    # Session and mount the configured adapters lazily.
    session_adapters: Sequence[HTTPAdapterMountConfig] | None = None

    # Internal: context-managed session and ownership flag.
    _request_manager: RequestManager = field(
        init=False,
        repr=False,
        compare=False,
    )

    # -- Class Defaults (Centralized) -- #

    DEFAULT_PAGE_PARAM: ClassVar[str] = 'page'
    DEFAULT_SIZE_PARAM: ClassVar[str] = 'per_page'
    DEFAULT_START_PAGE: ClassVar[int] = 1
    DEFAULT_PAGE_SIZE: ClassVar[int] = 100
    DEFAULT_CURSOR_PARAM: ClassVar[str] = 'cursor'
    DEFAULT_LIMIT_PARAM: ClassVar[str] = 'limit'

    # Retry defaults (only used if a policy is provided)
    DEFAULT_RETRY_MAX_ATTEMPTS: ClassVar[int] = RetryStrategy.DEFAULT_ATTEMPTS
    DEFAULT_RETRY_BACKOFF: ClassVar[float] = RetryStrategy.DEFAULT_BACKOFF
    DEFAULT_RETRY_ON: ClassVar[tuple[int, ...]] = tuple(
        RetryManager.DEFAULT_STATUS_CODES,
    )

    # Cap for jittered backoff sleeps (seconds)
    DEFAULT_RETRY_CAP: ClassVar[float] = RetryManager.DEFAULT_CAP

    # Default timeout applied when callers do not explicitly provide one.
    DEFAULT_TIMEOUT: ClassVar[float] = 10.0

    # -- Magic Methods (Object Lifecycle) -- #

    def __post_init__(self) -> None:
        """
        Validate inputs and finalize immutable state.

        Ensures ``base_url`` is absolute, copies and validates endpoint
        mappings, wraps them in a read-only proxy, and synthesizes a
        session factory when only adapter configs are provided.

        Raises
        ------
        ValueError
            If ``base_url`` is not absolute or endpoints are invalid.
        """
        # Validate base_url is absolute.
        parts = urlsplit(self.base_url)
        if not parts.scheme or not parts.netloc:
            raise ValueError(
                'base_url must be absolute, e.g. "https://api.example.com"',
            )

        # Defensive copy + validate endpoints with concise comprehension.
        eps = dict(self.endpoints)
        invalid = [
            (k, v)
            for k, v in eps.items()
            if not (isinstance(k, str) and isinstance(v, str) and v)
        ]
        if invalid:
            sample = invalid[:3]
            msg = (
                'endpoints must map str -> non-empty str; '
                f'invalid entries: {sample}'
            )
            raise ValueError(msg)
        # Wrap in a read-only mapping to ensure immutability
        object.__setattr__(self, 'endpoints', MappingProxyType(eps))

        # If both session and factory are provided, prefer explicit session.
        if self.session is not None and self.session_factory is not None:
            object.__setattr__(self, 'session_factory', None)

        # Normalize adapter configs to tuples for immutability.
        if self.session_adapters:
            adapters_cfg = tuple(self.session_adapters)
            object.__setattr__(self, 'session_adapters', adapters_cfg)
        else:
            object.__setattr__(self, 'session_adapters', None)

        manager = RequestManager(
            retry=self.retry,
            retry_network_errors=self.retry_network_errors,
            default_timeout=self.DEFAULT_TIMEOUT,
            session=self.session,
            session_factory=self.session_factory,
            session_adapters=self.session_adapters,
            retry_cap=self.DEFAULT_RETRY_CAP,
        )
        object.__setattr__(self, '_request_manager', manager)

    # -- Magic Methods (Context Manager Protocol) -- #

    def __enter__(self) -> Self:
        """
        Enter the runtime context related to this object.

        Returns
        -------
        Self
            The client instance.
        """
        self._request_manager.__enter__()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """
        Exit the runtime context related to this object.

        Parameters
        ----------
        exc_type : type[BaseException] | None
            Exception type if raised, else ``None``.
        exc : BaseException | None
            Exception instance if raised, else ``None``.
        tb : TracebackType | None
            Traceback if exception raised, else ``None``.
        """
        self._request_manager.__exit__(exc_type, exc, tb)

    # -- Internal Instance Methods -- #

    def _build_pagination_client(
        self,
        *,
        pagination: PaginationInput,
        sleep_seconds: float,
        rate_limit_overrides: RateLimitOverrides,
    ) -> PaginationClient:
        """
        Create a :class:`PaginationClient` wired to the request manager.

        Parameters
        ----------
        pagination : PaginationInput
            Pagination configuration mapping or :class:`PaginationConfig`.
        sleep_seconds : float
            Number of seconds to sleep between requests.
        rate_limit_overrides : RateLimitOverrides
            Overrides for rate limiting.

        Returns
        -------
        PaginationClient
            Configured pagination helper instance.
        """
        effective_sleep = self._resolve_sleep_seconds(
            sleep_seconds,
            self.rate_limit,
            rate_limit_overrides,
        )
        rate_limiter = (
            RateLimiter.fixed(effective_sleep) if effective_sleep > 0 else None
        )
        return PaginationClient(
            pagination=pagination,
            fetch=self._fetch_page,
            rate_limiter=rate_limiter,
        )

    def _fetch_page(
        self,
        url_: Url,
        request: RequestOptions,
        page_index: int | None,
    ) -> JSONData:
        """
        Fetch a single page using shared pagination guardrails.

        Parameters
        ----------
        url_ : Url
            Absolute URL to request.
        request : RequestOptions
            Request metadata produced by ``Paginator``.
        page_index : int | None
            Index of the page being fetched.

        Returns
        -------
        JSONData
            Parsed response payload.

        Raises
        ------
        PaginationError
            If the request fails.
        """
        call_kw = request.as_kwargs()
        try:
            return self.get(url_, **call_kw)
        except ApiRequestError as exc:
            raise PaginationError(
                url=url_,
                status=exc.status,
                attempts=exc.attempts,
                retried=exc.retried,
                retry_policy=exc.retry_policy,
                cause=exc,
                page=page_index,
            ) from exc

    # -- Instance Methods (HTTP Requests ) -- #

    def get(
        self,
        url: Url,
        **kwargs: Any,
    ) -> JSONData:
        """
        Wrap ``request('GET', ...)`` for convenience.

        Parameters
        ----------
        url : Url
            Absolute URL to request.
        **kwargs : Any
            Additional keyword arguments forwarded to ``requests``
            (e.g., ``params``, ``headers``).

        Returns
        -------
        JSONData
            Parsed JSON payload or fallback structure matching
            :func:`etlplus.ops.extract.extract_from_api` semantics.
        """
        return self._request_manager.get(url, **kwargs)

    def post(
        self,
        url: Url,
        **kwargs: Any,
    ) -> JSONData:
        """
        Wrap ``request('POST', ...)`` for convenience.

        Parameters
        ----------
        url : Url
            Absolute URL to request.
        **kwargs : Any
            Additional keyword arguments forwarded to ``requests``
            (e.g., ``params``, ``headers``, ``json``).

        Returns
        -------
        JSONData
            Parsed JSON payload or fallback structure matching
            :func:`etlplus.ops.extract.extract_from_api` semantics.
        """
        return self._request_manager.post(url, **kwargs)

    def request(
        self,
        method: str,
        url: Url,
        **kwargs: Any,
    ) -> JSONData:
        """
        Execute an HTTP request using the client's retry and session settings.

        Parameters
        ----------
        method : str
            HTTP method to invoke (``'GET'``, ``'POST'``, etc.).
        url : Url
            Absolute URL to request.
        **kwargs : Any
            Additional keyword arguments forwarded to ``requests``
            (e.g., ``params``, ``headers``, ``json``).

        Returns
        -------
        JSONData
            Parsed JSON payload or fallback structure matching
            :func:`etlplus.ops.extract.extract_from_api` semantics.
        """
        return self._request_manager.request(method, url, **kwargs)

    # -- Instance Methods (HTTP Responses) -- #

    def paginate(
        self,
        endpoint_key: str,
        *,
        path_parameters: Mapping[str, str] | None = None,
        query_parameters: Mapping[str, str] | None = None,
        pagination: PaginationInput = None,
        request: RequestOptions | None = None,
        sleep_seconds: float = 0.0,
        rate_limit_overrides: RateLimitOverrides = None,
    ) -> JSONData:
        """
        Paginate by endpoint key.

        Builds the URL via ``self.url(...)`` and delegates to ``paginate_url``.

        Parameters
        ----------
        endpoint_key : str
            Key into the ``endpoints`` mapping whose relative path will be
            resolved against ``base_url``.
        path_parameters : Mapping[str, str] | None
            Values to substitute into placeholders in the endpoint path.
        query_parameters : Mapping[str, str] | None
            Query parameters to append (merged with any already present on
            ``base_url``).
        pagination : PaginationInput, optional
            Pagination configuration mapping or :class:`PaginationConfig`.
        request : RequestOptions | None, optional
            Pre-built request metadata snapshot (params/headers/timeout).
        sleep_seconds : float
            Time to sleep between requests.
        rate_limit_overrides : RateLimitOverrides, optional
            Optional per-call overrides merged with ``self.rate_limit`` when
            deriving pacing.

        Returns
        -------
        JSONData
            Raw JSON object for non-paginated calls, or a list of record
            dicts aggregated across pages for paginated calls.
        """
        url = self.url(
            endpoint_key,
            path_parameters=path_parameters,
            query_parameters=query_parameters,
        )
        return self.paginate_url(
            url,
            pagination=pagination,
            request=request,
            sleep_seconds=sleep_seconds,
            rate_limit_overrides=rate_limit_overrides,
        )

    def paginate_iter(
        self,
        endpoint_key: str,
        *,
        path_parameters: Mapping[str, str] | None = None,
        query_parameters: Mapping[str, str] | None = None,
        pagination: PaginationInput = None,
        request: RequestOptions | None = None,
        sleep_seconds: float = 0.0,
        rate_limit_overrides: RateLimitOverrides = None,
    ) -> Iterator[JSONDict]:
        """
        Stream records for a registered endpoint using pagination.

        Summary
        -------
        Generator variant of ``paginate`` that yields record dicts across
        pages instead of aggregating them into a list.

        Parameters
        ----------
        endpoint_key : str
            Key into the ``endpoints`` mapping whose relative path will be
            resolved against ``base_url``.
        path_parameters : Mapping[str, str] | None
            Values to substitute into placeholders in the endpoint path.
        query_parameters : Mapping[str, str] | None
            Query parameters to append (merged with any already present).
        pagination : PaginationInput, optional
            Pagination configuration mapping or :class:`PaginationConfig`.
        request : RequestOptions | None, optional
            Pre-built request metadata snapshot (params/headers/timeout).
        sleep_seconds : float
            Time to sleep between requests.
        rate_limit_overrides : RateLimitOverrides, optional
            Optional per-call overrides merged with ``self.rate_limit`` when
            deriving pacing.

        Yields
        ------
        JSONDict
            Record dictionaries extracted from each page.
        """
        url = self.url(
            endpoint_key,
            path_parameters=path_parameters,
            query_parameters=query_parameters,
        )
        yield from self.paginate_url_iter(
            url=url,
            pagination=pagination,
            request=request,
            sleep_seconds=sleep_seconds,
            rate_limit_overrides=rate_limit_overrides,
        )

    def paginate_url(
        self,
        url: Url,
        pagination: PaginationInput = None,
        *,
        request: RequestOptions | None = None,
        sleep_seconds: float = 0.0,
        rate_limit_overrides: RateLimitOverrides = None,
    ) -> JSONData:
        """
        Paginate API responses for an absolute URL and aggregate records.

        Parameters
        ----------
        url : Url
            Absolute URL to paginate.
        pagination : PaginationInput, optional
            Pagination configuration mapping or :class:`PaginationConfig`.
        request : RequestOptions | None, optional
            Optional request snapshot with existing params/headers/timeout.
        sleep_seconds : float
            Time to sleep between requests.
        rate_limit_overrides : RateLimitOverrides, optional
            Optional per-call overrides merged with ``self.rate_limit`` when
            deriving pacing.

        Returns
        -------
        JSONData
            Raw JSON object for non-paginated calls, or a list of record
            dicts aggregated across pages for paginated calls.
        """
        # Normalize pagination config for typed access.
        if pagination is not None and not isinstance(pagination, Mapping):
            ptype = getattr(pagination, 'type', None)
        else:
            pg_map = cast(Mapping[str, Any] | None, pagination)
            ptype = Paginator.detect_type(pg_map, default=None)
        request_obj = request or RequestOptions()

        # Preserve raw JSON behavior for non-paginated and unknown types.
        if ptype is None:
            return self.get(url, **request_obj.as_kwargs())

        # For known pagination types, delegate through paginate_url_iter to
        # preserve subclass overrides (tests rely on this shim behavior).
        # Pass the composed ``request_obj`` as the baseline snapshot and
        # avoid re-specifying params/headers/timeout so pagination glue
        # does not re-merge the same values a second time.
        return list(
            self.paginate_url_iter(
                url,
                pagination=pagination,
                request=request_obj,
                sleep_seconds=sleep_seconds,
                rate_limit_overrides=rate_limit_overrides,
            ),
        )

    def paginate_url_iter(
        self,
        url: Url,
        pagination: PaginationInput = None,
        *,
        request: RequestOptions | None = None,
        sleep_seconds: float = 0.0,
        rate_limit_overrides: RateLimitOverrides = None,
    ) -> Iterator[JSONDict]:
        """
        Stream records by paginating an absolute URL.

        Parameters
        ----------
        url : Url
            Absolute URL to paginate.
        pagination : PaginationInput, optional
            Pagination configuration mapping or :class:`PaginationConfig`.
        request : RequestOptions | None, optional
            Optional request snapshot reused across pages.
        sleep_seconds : float
            Time to sleep between requests.
        rate_limit_overrides : RateLimitOverrides, optional
            Optional per-call overrides merged with ``self.rate_limit`` when
            deriving pacing.

        Yields
        ------
        JSONDict
            Record dictionaries extracted from each page.
        """
        base_request = request or RequestOptions()

        runner = self._build_pagination_client(
            pagination=pagination,
            sleep_seconds=sleep_seconds,
            rate_limit_overrides=rate_limit_overrides,
        )
        yield from runner.iterate(
            url,
            request=base_request,
        )

    # -- Instance Methods (Endpoints)-- #

    def url(
        self,
        endpoint_key: str,
        path_parameters: Mapping[str, Any] | None = None,
        query_parameters: Mapping[str, Any] | None = None,
    ) -> str:
        """
        Build an absolute URL for a registered endpoint.

        Parameters
        ----------
        endpoint_key : str
            Key into the ``endpoints`` mapping whose relative path will be
            resolved against ``base_url``.
        path_parameters : Mapping[str, Any] | None, optional
            Values to substitute into placeholders in the endpoint path.
            Placeholders must be written as ``{placeholder}`` in the relative
            path. Each substituted value is percent-encoded as a single path
            segment (slashes are encoded) to prevent path traversal.
        query_parameters : Mapping[str, Any] | None, optional
            Query parameters to append (and merge with any already present on
            ``base_url``). Values are percent-encoded and combined using
            ``application/x-www-form-urlencoded`` rules.

        Returns
        -------
        str
            Constructed absolute URL.

        Raises
        ------
        KeyError
            If *endpoint_key* is unknown or a required placeholder in the path
            has no corresponding entry in *path_parameters*.
        ValueError
            If the path template is invalid.

        Examples
        --------
        >>> ep = EndpointClient(
        ...     base_url='https://api.example.com/v1',
        ...     endpoints={
        ...         'user': '/users/{id}',
        ...         'search': '/users'
        ...     }
        ... )
        >>> ep.url('user', path_parameters={'id': '42'})
        'https://api.example.com/v1/users/42'
        >>> ep.url('search', query_parameters={'q': 'Jane Doe', 'page': '2'})
        'https://api.example.com/v1/users?q=Jane+Doe&page=2'
        """
        if endpoint_key not in self.endpoints:
            raise KeyError(f'Unknown endpoint_key: {endpoint_key!r}')

        rel_path = self.endpoints[endpoint_key]

        # Substitute path parameters if provided.
        if '{' in rel_path:
            try:
                encoded = (
                    {
                        k: quote(str(v), safe='')
                        for k, v in path_parameters.items()
                    }
                    if path_parameters
                    else {}
                )
                rel_path = rel_path.format(**encoded)
            except KeyError as e:
                missing = e.args[0]
                raise KeyError(
                    f'Missing path parameter for placeholder: {missing!r}',
                ) from None
            except ValueError as e:
                raise ValueError(
                    f'Invalid path template {rel_path!r}: {e}',
                ) from None

        # Build final absolute URL, honoring any client base_path prefix.
        parts = urlsplit(self.base_url)
        base_url_path = parts.path.rstrip('/')
        extra = self.base_path
        extra_norm = ('/' + extra.lstrip('/')) if extra else ''
        composed_base = (
            base_url_path + extra_norm if (base_url_path or extra_norm) else ''
        )
        rel_norm = '/' + rel_path.lstrip('/')
        path = (composed_base + rel_norm) if composed_base else rel_norm

        # Merge base query with provided query_parameters.
        base_q = parse_qsl(parts.query, keep_blank_values=True)
        add_q = list((query_parameters or {}).items())
        qs = urlencode(base_q + add_q, doseq=True)

        return urlunsplit(
            (parts.scheme, parts.netloc, path, qs, parts.fragment),
        )

    # -- Static Methods -- #

    @staticmethod
    def apply_sleep(
        sleep_seconds: float,
        *,
        sleeper: Callable[[float], None] | None = None,
    ) -> None:
        """
        Sleep for the specified seconds if positive.

        The optional *sleeper* is useful for tests (e.g., pass
        ``lambda s: None``). Defaults to using time.sleep when not provided.

        Parameters
        ----------
        sleep_seconds : float
            Number of seconds to sleep; no-op if non-positive.
        sleeper : Callable[[float], None] | None, optional
            Optional sleeper function taking seconds as input.
        """
        if sleep_seconds and sleep_seconds > 0:
            if sleeper is None:
                time.sleep(sleep_seconds)
            else:
                sleeper(sleep_seconds)

    # -- Internal Static Methods -- #

    @staticmethod
    def _resolve_sleep_seconds(
        explicit: float,
        rate_limit: RateLimitConfigMap | None,
        overrides: RateLimitOverrides = None,
    ) -> float:
        """
        Derive the effective sleep interval honoring rate-limit config.

        Parameters
        ----------
        explicit : float
            Explicit sleep seconds provided by the caller.
        rate_limit : RateLimitConfigMap | None
            Client-wide rate limit configuration.
        overrides : RateLimitOverrides, optional
            Per-call overrides that take precedence over *rate_limit*.

        Returns
        -------
        float
            The resolved sleep seconds to apply between requests.
        """
        if explicit and explicit > 0:
            return explicit
        return RateLimiter.resolve_sleep_seconds(
            rate_limit=rate_limit,
            overrides=overrides,
        )
