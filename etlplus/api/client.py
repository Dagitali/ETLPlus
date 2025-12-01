"""
etlplus.api.client module.

Endpoint client utilities for registering endpoint paths, composing URLs, and
paginating API responses with optional retries and rate limiting.

Notes
-----
- Centralized types live in :mod:`etlplus.api.types` (re-exported from
    :mod:`etlplus.api`). Prefer importing ``PaginationConfig``,
    ``RateLimitConfig``, and ``RetryPolicy`` rather than redefining ad-hoc
    dicts.
- Pagination requires a ``PaginationConfig``. See
    :class:`PagePaginationConfig` and :class:`CursorPaginationConfig` for the
    accepted shapes.

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
from dataclasses import dataclass
from dataclasses import field
from types import MappingProxyType
from types import TracebackType
from typing import Any
from typing import Callable
from typing import cast
from typing import ClassVar
from typing import Iterator
from typing import Mapping
from typing import Self
from urllib.parse import parse_qsl
from urllib.parse import quote
from urllib.parse import urlencode
from urllib.parse import urlsplit
from urllib.parse import urlunsplit

import requests  # type: ignore[import]

from ..extract import extract as _extract
from .errors import ApiAuthError
from .errors import ApiRequestError
from .errors import PaginationError
from .request import compute_sleep_seconds
from .request import RateLimitMap
from .request import RetryManager
from .response import PaginationConfig
from .response import Paginator
from .transport import build_http_adapter
from .transport import HTTPAdapterMountConfig
from .types import JSONData
from .types import JSONList
from .types import RetryPolicy


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
    base_url : str
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
    rate_limit : RateLimitConfig | None, optional
        Optional client-wide rate limit used to derive an inter-request
        delay when an explicit ``sleep_seconds`` isn't supplied.
    session : requests.Session | None, optional
        Explicit HTTP session for all requests.
    session_factory : Callable[[], requests.Session] | None, optional
        Factory used to lazily create a session. Ignored if ``session`` is
        provided.
    session_adapters : list[HTTPAdapterMountConfig] | None, optional
        Adapter mount configuration(s) used to build a session lazily when
        neither ``session`` nor ``session_factory`` is supplied.

    Attributes
    ----------
    base_url : str
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
    rate_limit : RateLimitConfig | None
        Client-wide rate limit configuration (may be ``None``).
    session : requests.Session | None
        Explicit HTTP session used for requests when provided.
    session_factory : Callable[[], requests.Session] | None
        Lazily invoked factory producing a session when needed.
    session_adapters : list[HTTPAdapterMountConfig] | None
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

    base_url: str
    endpoints: Mapping[str, str]
    base_path: str | None = None

    # Optional retry configuration (constructor parameter; object is frozen)
    retry: RetryPolicy | None = None
    retry_network_errors: bool = False
    # Optional client-wide rate limit configuration
    rate_limit: RateLimitMap | None = None

    # Optional HTTP session or factory
    session: requests.Session | None = None
    session_factory: Callable[[], requests.Session] | None = None

    # Optional HTTPAdapter mount configuration(s) for transport-level retries
    # and connection pooling. If provided and neither `session` nor
    # `session_factory` is supplied, a factory is synthesized to create a
    # Session and mount the configured adapters lazily.
    session_adapters: list[HTTPAdapterMountConfig] | None = None

    # Internal: context-managed session and ownership flag.
    _ctx_session: Any | None = field(default=None, repr=False, compare=False)
    _ctx_owns_session: bool = field(
        default=False, repr=False, compare=False,
    )

    # -- Class Defaults (Centralized) -- #

    DEFAULT_PAGE_PARAM: ClassVar[str] = 'page'
    DEFAULT_SIZE_PARAM: ClassVar[str] = 'per_page'
    DEFAULT_START_PAGE: ClassVar[int] = 1
    DEFAULT_PAGE_SIZE: ClassVar[int] = 100
    DEFAULT_CURSOR_PARAM: ClassVar[str] = 'cursor'
    DEFAULT_LIMIT_PARAM: ClassVar[str] = 'limit'

    # Retry defaults (only used if a policy is provided)
    DEFAULT_RETRY_MAX_ATTEMPTS: ClassVar[int] = 3
    DEFAULT_RETRY_BACKOFF: ClassVar[float] = 0.5
    DEFAULT_RETRY_ON: ClassVar[tuple[int, ...]] = (429, 502, 503, 504)

    # Cap for jittered backoff sleeps (seconds)
    DEFAULT_RETRY_CAP: ClassVar[float] = 30.0

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

        # If no session/factory provided but adapter configs are, synthesize
        # a factory that builds a Session and mounts adapters.
        if (
            self.session is None
            and self.session_factory is None
            and self.session_adapters
        ):
            adapters_cfg = list(self.session_adapters)

            def _factory() -> requests.Session:
                s = requests.Session()
                for cfg in adapters_cfg:
                    prefix = cfg.get('prefix', 'https://')
                    try:
                        adapter = build_http_adapter(cfg)
                        s.mount(prefix, adapter)
                    except (ValueError, TypeError, AttributeError):
                        # If mounting fails for any reason, continue so that
                        # at least a default Session is returned.
                        continue
                return s

            object.__setattr__(self, 'session_factory', _factory)

    # -- Magic Methods (Context Manager Protocol) -- #

    def __enter__(self) -> Self:
        """
        Enter a context where a session is managed by the client.

        Returns
        -------
        Self
            The client itself with an active session bound for the context.

        Notes
        -----
        - Explicit ``session`` is reused and not closed on exit.
        - When ``session_factory`` exists it's invoked and the session
          closed on exit.
        - Otherwise, a default ``requests.Session`` is created and closed.
        """
        if self._ctx_session is not None:
            return self

        if self.session is not None:
            object.__setattr__(self, '_ctx_session', self.session)
            object.__setattr__(self, '_ctx_owns_session', False)
            return self

        if self.session_factory is not None:
            s = self.session_factory()
            object.__setattr__(self, '_ctx_session', s)
            object.__setattr__(self, '_ctx_owns_session', True)
            return self

        s = requests.Session()
        object.__setattr__(self, '_ctx_session', s)
        object.__setattr__(self, '_ctx_owns_session', True)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """
        Exit the managed-session context and close if owned.

        Ensures any session created by the context is closed and internal
        context state is cleared.
        """
        s = self._ctx_session
        owns = self._ctx_owns_session
        if s is not None and owns:
            try:
                close_attr = getattr(s, 'close', None)
                if close_attr is not None:
                    try:
                        # Some objects may expose a non-callable 'close'
                        # attribute. Guard the call to avoid TypeError.
                        close_attr()  # type: ignore[misc]
                    except TypeError:
                        pass
            finally:
                object.__setattr__(self, '_ctx_session', None)
                object.__setattr__(self, '_ctx_owns_session', False)
        else:
            # Ensure cleared even if we didn't own it
            object.__setattr__(self, '_ctx_session', None)
            object.__setattr__(self, '_ctx_owns_session', False)

    # -- Protected Instance Methods -- #

    def _extract_with_retry(
        self,
        url: str,
        **kw: Any,
    ) -> JSONData:
        """
        Execute an API GET with optional retry policy.

        Retries only apply when a retry policy is configured on the client.
        Without a policy, a single attempt is performed.

        Parameters
        ----------
        url : str
            Absolute URL to fetch.
        **kw : Any
            Keyword arguments forwarded to ``requests.get`` via
            :func:`etlplus.extract.extract`.

        Returns
        -------
        JSONData
            Parsed JSON payload.

        Raises
        ------
        ApiAuthError
            If authentication or authorization ultimately fails
            (for example, with a 401 or 403 status).
        ApiRequestError
            If all retry attempts fail (or a single attempt fails with no
            retry policy configured) for other HTTP statuses or network
            errors.
        """
        # Determine session to use for this request. Prefer context-managed
        # session when present.
        sess = self._get_active_session()

        policy: RetryPolicy | None = self.retry
        if not policy:
            if sess is not None:
                kw = dict(kw)
                kw['session'] = sess
            try:
                return _extract('api', url, **kw)
            except requests.RequestException as e:  # pragma: no cover (net)
                status = getattr(
                    getattr(e, 'response', None),
                    'status_code',
                    None,
                )
                retried = False
                if status in {401, 403}:
                    raise ApiAuthError(
                        url=url,
                        status=status,
                        attempts=1,
                        retried=retried,
                        retry_policy=None,
                        cause=e,
                    ) from e
                raise ApiRequestError(
                    url=url,
                    status=status,
                    attempts=1,
                    retried=retried,
                    retry_policy=None,
                    cause=e,
                ) from e

        retry_mgr = RetryManager(
            policy=policy,
            retry_network_errors=self.retry_network_errors,
            cap=self.DEFAULT_RETRY_CAP,
        )
        if sess is not None:
            kw = dict(kw)
            kw['session'] = sess

        return retry_mgr.run_with_retry(
            lambda url, **kw: _extract('api', url, **kw),
            url,
            **kw,
        )

    def _get_active_session(self) -> requests.Session | None:
        """
        Return the currently active session if available.

        Prefers the context-managed session, then an explicit bound
        ``session``, then lazily creates one via ``session_factory`` when
        available. Returns ``None`` when no session can be obtained.

        Returns
        -------
        requests.Session | None
            The session to use for an outgoing request, if any.
        """
        if self._ctx_session is not None:
            return self._ctx_session
        if self.session is not None:
            return self.session
        if self.session_factory is not None:
            try:
                return self.session_factory()
            except (RuntimeError, TypeError):  # pragma: no cover - defensive
                return None
        return None

    # -- Instance Methods -- #

    def paginate(
        self,
        endpoint_key: str,
        *,
        path_parameters: Mapping[str, str] | None = None,
        query_parameters: Mapping[str, str] | None = None,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, Any] | None = None,
        timeout: float | int | None = None,
        pagination: PaginationConfig | None = None,
        sleep_seconds: float = 0.0,
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
        params : Mapping[str, Any] | None
            Query parameters to include in the request.
        headers : Mapping[str, Any] | None
            Headers to include in the request.
        timeout : float | int | None
            Timeout for the request.
        pagination : PaginationConfig | None
            Pagination configuration.
        sleep_seconds : float
            Time to sleep between requests.

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

        # If no pagination provided, preserve single-request behavior.
        if not pagination or not pagination.get('type'):
            kw = EndpointClient.build_request_kwargs(
                params=params, headers=headers, timeout=timeout,
            )
            return self._extract_with_retry(url, **kw)

        # Collect from iterator for paginated streaming ergonomics.
        return list(
            self.paginate_url_iter(
                url=url,
                params=params,
                headers=headers,
                timeout=timeout,
                pagination=pagination,
                sleep_seconds=sleep_seconds,
            ),
        )

    def paginate_iter(
        self,
        endpoint_key: str,
        *,
        path_parameters: Mapping[str, str] | None = None,
        query_parameters: Mapping[str, str] | None = None,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, Any] | None = None,
        timeout: float | int | None = None,
        pagination: PaginationConfig | None = None,
        sleep_seconds: float = 0.0,
    ) -> Iterator[dict]:
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
        params : Mapping[str, Any] | None
            Query parameters to include in each request.
        headers : Mapping[str, Any] | None
            Headers to include in each request.
        timeout : float | int | None
            Timeout for each request.
        pagination : PaginationConfig | None
            Pagination configuration.
        sleep_seconds : float
            Time to sleep between requests.

        Yields
        ------
        dict
            Record dictionaries extracted from each page.
        """
        url = self.url(
            endpoint_key,
            path_parameters=path_parameters,
            query_parameters=query_parameters,
        )
        yield from self.paginate_url_iter(
            url=url,
            params=params,
            headers=headers,
            timeout=timeout,
            pagination=pagination,
            sleep_seconds=sleep_seconds,
        )

    def paginate_url(
        self,
        url: str,
        params: Mapping[str, Any] | None,
        headers: Mapping[str, Any] | None,
        timeout: float | int | None,
        pagination: PaginationConfig | None,
        *,
        sleep_seconds: float = 0.0,
    ) -> JSONData:
        """
        Paginate API responses for an absolute URL and aggregate records.

        Parameters
        ----------
        url : str
            Absolute URL to paginate.
        params : Mapping[str, Any] | None
            Query parameters to include in the request.
        headers : Mapping[str, Any] | None
            Headers to include in the request.
        timeout : float | int | None
            Timeout for the request.
        pagination : PaginationConfig | None
            Pagination configuration.
        sleep_seconds : float
            Time to sleep between requests.

        Returns
        -------
        JSONData
            Raw JSON object for non-paginated calls, or a list of record
            dicts aggregated across pages for paginated calls.
        """
        # Normalize pagination config for typed access.
        pg: dict[str, Any] = cast(dict[str, Any], pagination or {})
        ptype = pg.get('type') if pagination else None

        # Preserve raw JSON behavior for non-paginated and unknown types.
        if not ptype or ptype not in {'page', 'offset', 'cursor'}:
            kw = EndpointClient.build_request_kwargs(
                params=params, headers=headers, timeout=timeout,
            )
            return self._extract_with_retry(url, **kw)

        # For known pagination types, collect from the generator.
        return list(
            self.paginate_url_iter(
                url=url,
                params=params,
                headers=headers,
                timeout=timeout,
                pagination=pagination,
                sleep_seconds=sleep_seconds,
            ),
        )

    def paginate_url_iter(
        self,
        url: str,
        params: Mapping[str, Any] | None,
        headers: Mapping[str, Any] | None,
        timeout: float | int | None,
        pagination: PaginationConfig | None,
        *,
        sleep_seconds: float = 0.0,
    ) -> Iterator[dict]:
        """
        Stream records by paginating an absolute URL.

        Parameters
        ----------
        url : str
            Absolute URL to paginate.
        params : Mapping[str, Any] | None
            Query parameters to include in each request.
        headers : Mapping[str, Any] | None
            Headers to include in each request.
        timeout : float | int | None
            Timeout for each request.
        pagination : PaginationConfig | None
            Pagination configuration.
        sleep_seconds : float
            Time to sleep between requests.

        Yields
        ------
        dict
            Record dictionaries extracted from each page.
        """
        # Normalize pagination config for typed access.
        pg: dict[str, Any] = cast(dict[str, Any], pagination or {})
        ptype = pg.get('type') if pagination else None

        # No pagination type or unknown type: single request, coalesce, yield.
        if not ptype or ptype not in {'page', 'offset', 'cursor'}:
            kw = EndpointClient.build_request_kwargs(
                params=params,
                headers=headers,
                timeout=timeout,
            )
            page_data = self._extract_with_retry(url, **kw)
            records_path = pg.get('records_path')
            yield from Paginator.coalesce_records(page_data, records_path)
            return

        # Determine effective sleep seconds.
        effective_sleep = (
            sleep_seconds
            if sleep_seconds and sleep_seconds > 0
            else compute_sleep_seconds(self.rate_limit, None)
        )

        def _fetch(
            url_: str,
            params_: Mapping[str, Any] | None,
            page_index: int | None,
        ) -> JSONData:
            call_kw = EndpointClient.build_request_kwargs(
                params=params_,
                headers=headers,
                timeout=timeout,
            )
            try:
                return self._extract_with_retry(url_, **call_kw)
            except ApiRequestError as e:
                raise PaginationError(
                    url=url_,
                    status=e.status,
                    attempts=e.attempts,
                    retried=e.retried,
                    retry_policy=e.retry_policy,
                    cause=e,
                    page=page_index,
                ) from e

        paginator = Paginator.from_config(
            cast(PaginationConfig, pg),
            fetch=_fetch,
            sleep_func=EndpointClient.apply_sleep,
            sleep_seconds=effective_sleep,
        )

        yield from paginator.paginate_iter(url, params=params)

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
            If ``endpoint_key`` is unknown or a required placeholder in the
            path has no corresponding entry in ``path_parameters``.
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

        The optional ``sleeper`` is useful for tests (e.g., pass
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

    @staticmethod
    def build_request_kwargs(
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, Any] | None = None,
        timeout: float | int | None = None,
    ) -> dict[str, Any]:
        """
        Build kwargs for ``requests.get``.

        Only include keys that have non-empty values to keep kwargs tidy.

        Parameters
        ----------
        params : Mapping[str, Any] | None, optional
            Query parameters to include in the request.
        headers : Mapping[str, Any] | None, optional
            Headers to include in the request.
        timeout : float | int | None, optional
            Timeout for the request in seconds.

        Returns
        -------
        dict[str, Any]
            Dictionary of keyword arguments for ``requests.get``.
        """
        kw: dict[str, Any] = {}
        if params:
            kw['params'] = dict(params)
        if headers:
            kw['headers'] = dict(headers)
        if timeout is not None:
            kw['timeout'] = timeout
        return kw

    @staticmethod
    def coalesce_records(
        x: Any,
        records_path: str | None,
    ) -> JSONList:
        """
        Coalesce JSON page payloads into a list of dicts.

        Supports dotted path extraction via ``records_path`` and handles lists,
        maps, and scalars by coercing non-dict items into ``{'value': x}``.

        Parameters
        ----------
        x : Any
            The JSON payload from an API response.
        records_path : str | None
            Dotted path to the records within the payload.

        Returns
        -------
        JSONList
            List of record dicts extracted from the payload.
        """
        return Paginator.coalesce_records(x, records_path)

    @staticmethod
    def next_cursor_from(
        data_obj: Any,
        path: str | None,
    ) -> str | int | None:
        """
        Extract a cursor value from a JSON payload using a dotted path.

        Parameters
        ----------
        data_obj : Any
            The JSON payload object (expected to be a dict).
        path : str | None
            Dotted path within the payload that points to the next cursor.

        Returns
        -------
        str | int | None
            The extracted cursor value if present and of type str or int;
            otherwise None.
        """
        return Paginator.next_cursor_from(data_obj, path)
