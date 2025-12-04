"""
``etlplus.api.client`` module.

Endpoint client utilities for registering endpoint paths, composing URLs, and
paginating API responses with optional retries and rate limiting.

Notes
-----
- Pagination/record aliases are exported from :mod:`etlplus.api.response` and
    retry-related types live in :mod:`etlplus.api.request`.
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
from collections.abc import Callable
from collections.abc import Iterator
from collections.abc import Mapping
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
from requests import Response  # type: ignore[import]

from ..types import JSONData
from ..types import JSONDict
from ..types import JSONList
from ..types import JSONRecords
from .errors import ApiAuthError
from .errors import ApiRequestError
from .errors import PaginationError
from .request import RateLimitConfigMap
from .request import RetryManager
from .request import RetryPolicy
from .request import compute_sleep_seconds
from .response import PaginationConfigMap
from .response import PaginationType
from .response import Paginator
from .transport import HTTPAdapterMountConfig
from .transport import build_http_adapter


# SECTION: CONSTANTS ======================================================== #


_MISSING = object()


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
    rate_limit : RateLimitConfigMap | None, optional
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
    rate_limit : RateLimitConfigMap | None
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

    base_url: str
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
            except AttributeError:
                # Some callers provide mock objects with non-callable
                # ``close`` attributes—ignore those rather than bubble.
                pass
            finally:
                self._clear_managed_session()
        else:
            # Ensure cleared even if we didn't own it
            self._clear_managed_session()

    def _clear_managed_session(self) -> None:
        """Reset context-managed session bookkeeping."""
        object.__setattr__(self, '_ctx_session', None)
        object.__setattr__(self, '_ctx_owns_session', False)

    # -- Protected Instance Methods -- #

    # TODO: Remove this method.  Replace calls with :meth:`_get_with_retry`.
    def _extract_with_retry(
        self,
        url: str,
        **kw: Any,
    ) -> JSONData:
        """
        Backwards-compatible alias for :meth:`_get_with_retry`.

        Parameters
        ----------
        url : str
            Absolute URL to fetch.
        **kw : Any
            Keyword arguments forwarded to :meth:`_get_with_retry`.

        Returns
        -------
        JSONData
            Parsed payload produced by :meth:`_get_with_retry`.

        Notes
        -----
        Prefer :meth:`_get_with_retry`; this wrapper exists to avoid churn in
        older call sites and tests.
        """
        return self._get_with_retry(url, **kw)

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

    def _get_with_retry(
        self,
        url: str,
        **kw: Any,
    ) -> JSONData:
        """
        Execute an HTTP ``GET`` request with the client's retry semantics.

        Parameters
        ----------
        url : str
            Absolute URL to fetch.
        **kw : Any
            Keyword arguments forwarded to :meth:`request` (e.g., ``params``).

        Returns
        -------
        JSONData
            Parsed payload produced by :meth:`request`.

        Notes
        -----
        This method surfaces the more accurate name for what historically
        lived behind :meth:`_extract_with_retry`. Errors from
        :meth:`request` (``ApiAuthError``/``ApiRequestError``) propagate
        unchanged.
        """
        return self.request('GET', url, **kw)

    def _parse_response_payload(
        self,
        response: Response,
    ) -> JSONData:
        """
        Normalize ``requests`` responses into JSON-compatible payloads.

        Parameters
        ----------
        response : Response
            Raw ``requests`` response.

        Returns
        -------
        JSONData
            Parsed response payload.
        """
        content_type = response.headers.get('content-type', '').lower()
        if 'application/json' in content_type:
            try:
                payload: Any = response.json()
            except ValueError:
                return {
                    'content': response.text,
                    'content_type': content_type,
                }
            if isinstance(payload, dict):
                return cast(JSONDict, payload)
            if isinstance(payload, list):
                if all(isinstance(item, dict) for item in payload):
                    return cast(JSONList, payload)
                return [{'value': item} for item in payload]
            return {'value': payload}

        return {
            'content': response.text,
            'content_type': content_type,
        }

    def _request_once(
        self,
        method: str,
        url: str,
        *,
        session: requests.Session | None,
        timeout: Any,
        **kwargs: Any,
    ) -> JSONData:
        """
        Perform a single HTTP request and parse the response payload.

        Parameters
        ----------
        method : str
            HTTP method to invoke.
        url : str
            Absolute URL to request.
        session : requests.Session | None
            Session used for dispatch (if any).
        timeout : Any
            Timeout supplied to ``requests``.
        **kwargs : Any
            Additional keyword arguments forwarded to ``requests``.

        Returns
        -------
        JSONData
            Parsed response payload.
        """
        method_normalized = self._normalize_http_method(method)
        response = self._send_http_request(
            method_normalized,
            url,
            session=session,
            timeout=timeout,
            **kwargs,
        )
        response.raise_for_status()
        return self._parse_response_payload(response)

    def _request_with_retry(
        self,
        method: str,
        url: str,
        **kw: Any,
    ) -> JSONData:
        """
        Execute an HTTP request honoring the configured retry policy.

        Parameters
        ----------
        method : str
            HTTP method to invoke.
        url : str
            Absolute URL to request.
        **kw : Any
            Keyword arguments forwarded to :func:`requests.request`.

        Returns
        -------
        JSONData
            Parsed response payload.

        Raises
        ------
        ApiAuthError
            If authentication ultimately fails (401/403).
        ApiRequestError
            If retries are exhausted for other failures.
        """
        method_normalized = self._normalize_http_method(method)

        call_kwargs = dict(kw)
        supplied_timeout = call_kwargs.pop('timeout', _MISSING)
        timeout = self._resolve_timeout(supplied_timeout)
        user_session = call_kwargs.pop('session', None)
        # Determine session to use for this request. Prefer context-managed
        # session when present.
        sess = self._get_active_session() or user_session

        policy: RetryPolicy | None = self.retry
        if not policy:
            try:
                return self._request_once(
                    method_normalized,
                    url,
                    session=sess,
                    timeout=timeout,
                    **call_kwargs,
                )
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

        def _api_fetch(target_url: str, **call_kw: Any) -> JSONData:
            return self._request_once(
                method_normalized,
                target_url,
                session=sess,
                timeout=timeout,
                **call_kw,
            )

        return retry_mgr.run_with_retry(
            _api_fetch,
            url,
            **call_kwargs,
        )

    def _resolve_request_callable(
        self,
        session: requests.Session | None,
    ) -> Callable[..., Response]:
        """
        Resolve which callable should issue the HTTP request.

        Parameters
        ----------
        session : requests.Session | None
            Optional session provided by the caller or context manager.

        Returns
        -------
        Callable[..., Response]
            A callable mirroring ``requests.request``.

        Raises
        ------
        TypeError
            If a custom session does not expose a callable ``request``.
        """
        if session is not None:
            request_callable = getattr(session, 'request', None)
            if callable(request_callable):
                return request_callable
            raise TypeError('Session must expose a callable "request" method')
        return requests.request

    def _resolve_timeout(self, timeout: Any) -> Any:
        """
        Resolve timeout, applying the client default when unspecified.

        Parameters
        ----------
        timeout : Any
            Timeout value supplied by the caller.

        Returns
        -------
        Any
            Caller-supplied timeout or the client default.
        """
        return self.DEFAULT_TIMEOUT if timeout is _MISSING else timeout

    def _send_http_request(
        self,
        method: str,
        url: str,
        *,
        session: requests.Session | None,
        timeout: Any,
        **kwargs: Any,
    ) -> Response:
        """
        Dispatch an HTTP call using the provided or default session.

        Parameters
        ----------
        method : str
            HTTP method name.
        url : str
            Absolute URL to request.
        session : requests.Session | None
            Optional session bound to the client/context.
        timeout : Any
            Timeout forwarded to ``requests``.
        **kwargs : Any
            Additional keyword arguments for ``requests``.

        Returns
        -------
        Response
            Raw ``requests`` response for downstream parsing.
        """
        call_kwargs = {**kwargs, 'timeout': timeout}
        method_normalized = self._normalize_http_method(method)
        request_callable = self._resolve_request_callable(session)
        return request_callable(method_normalized, url, **call_kwargs)

    # -- Instance Methods (HTTP Requests )-- #

    def get(
        self,
        url: str,
        **kwargs: Any,
    ) -> JSONData:
        """
        Wrap ``request('GET', ...)`` for convenience.

        Parameters
        ----------
        url : str
            Absolute URL to request.
        **kwargs : Any
            Additional keyword arguments forwarded to ``requests``
            (e.g., ``params``, ``headers``).

        Returns
        -------
        JSONData
            Parsed JSON payload or fallback structure matching
            :func:`etlplus.extract.extract_from_api` semantics.
        """
        return self.request('GET', url, **kwargs)

    def post(
        self,
        url: str,
        **kwargs: Any,
    ) -> JSONData:
        """
        Wrap ``request('POST', ...)`` for convenience.

        Parameters
        ----------
        url : str
            Absolute URL to request.
        **kwargs : Any
            Additional keyword arguments forwarded to ``requests``
            (e.g., ``params``, ``headers``, ``json``).

        Returns
        -------
        JSONData
            Parsed JSON payload or fallback structure matching
            :func:`etlplus.extract.extract_from_api` semantics.
        """
        return self.request('POST', url, **kwargs)

    def request(
        self,
        method: str,
        url: str,
        **kwargs: Any,
    ) -> JSONData:
        """
        Execute an HTTP request using the client's retry and session settings.

        Parameters
        ----------
        method : str
            HTTP method to invoke (``'GET'``, ``'POST'``, etc.).
        url : str
            Absolute URL to request.
        **kwargs : Any
            Additional keyword arguments forwarded to ``requests``
            (e.g., ``params``, ``headers``, ``json``).

        Returns
        -------
        JSONData
            Parsed JSON payload or fallback structure matching
            :func:`etlplus.extract.extract_from_api` semantics.
        """
        return self._request_with_retry(method, url, **kwargs)

    # -- Instance Methods (HTTP Responses) -- #

    def paginate(
        self,
        endpoint_key: str,
        *,
        path_parameters: Mapping[str, str] | None = None,
        query_parameters: Mapping[str, str] | None = None,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, Any] | None = None,
        timeout: float | int | None = None,
        pagination: PaginationConfigMap | None = None,
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
        pagination : PaginationConfigMap | None
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
        return self.paginate_url(
            url,
            params=params,
            headers=headers,
            timeout=timeout,
            pagination=pagination,
            sleep_seconds=sleep_seconds,
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
        pagination: PaginationConfigMap | None = None,
        sleep_seconds: float = 0.0,
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
        params : Mapping[str, Any] | None
            Query parameters to include in each request.
        headers : Mapping[str, Any] | None
            Headers to include in each request.
        timeout : float | int | None
            Timeout for each request.
        pagination : PaginationConfigMap | None
            Pagination configuration.
        sleep_seconds : float
            Time to sleep between requests.

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
        pagination: PaginationConfigMap | None,
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
        pagination : PaginationConfigMap | None
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
        ptype = self._normalize_pagination_type(pg)

        # Preserve raw JSON behavior for non-paginated and unknown types.
        if ptype is None:
            kw = EndpointClient.build_request_kwargs(
                params=params, headers=headers, timeout=timeout,
            )
            return self._get_with_retry(url, **kw)

        # For known pagination types, collect from the generator.
        records: JSONRecords = list(
            self.paginate_url_iter(
                url=url,
                params=params,
                headers=headers,
                timeout=timeout,
                pagination=pagination,
                sleep_seconds=sleep_seconds,
            ),
        )
        return records

    def paginate_url_iter(
        self,
        url: str,
        params: Mapping[str, Any] | None,
        headers: Mapping[str, Any] | None,
        timeout: float | int | None,
        pagination: PaginationConfigMap | None,
        *,
        sleep_seconds: float = 0.0,
    ) -> Iterator[JSONDict]:
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
        pagination : PaginationConfigMap | None
            Pagination configuration.
        sleep_seconds : float
            Time to sleep between requests.

        Yields
        ------
        JSONDict
            Record dictionaries extracted from each page.
        """
        # Normalize pagination config for typed access.
        pg: dict[str, Any] = cast(dict[str, Any], pagination or {})
        ptype = self._normalize_pagination_type(pg)

        # No pagination type or unknown type: single request, coalesce, yield.
        if ptype is None:
            kw = EndpointClient.build_request_kwargs(
                params=params,
                headers=headers,
                timeout=timeout,
            )
            page_data = self._get_with_retry(url, **kw)
            records_path = pg.get('records_path')
            fallback_path = pg.get('fallback_path')
            yield from Paginator.coalesce_records(
                page_data,
                records_path,
                fallback_path,
            )
            return

        # Determine effective sleep seconds.
        effective_sleep = self._resolve_sleep_seconds(
            sleep_seconds,
            self.rate_limit,
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
                return self._get_with_retry(url_, **call_kw)
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
            cast(PaginationConfigMap, pg),
            fetch=_fetch,
            sleep_func=EndpointClient.apply_sleep,
            sleep_seconds=effective_sleep,
        )

        yield from paginator.paginate_iter(url, params=params)

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

    # -- Protected Static Methods -- #

    @staticmethod
    def _normalize_http_method(method: str | None) -> str:
        """
        Normalize HTTP method names to uppercase strings.

        Parameters
        ----------
        method : str | None
            Raw HTTP method value supplied by the caller.

        Returns
        -------
        str
            Uppercase HTTP method, defaulting to ``'GET'`` when falsy.
        """
        candidate = (method or '').strip().upper()
        return candidate or 'GET'

    @staticmethod
    def _normalize_pagination_type(
        config: Mapping[str, Any] | None,
    ) -> PaginationType | None:
        """
        Return a normalized ``PaginationType`` enum when possible.

        Parameters
        ----------
        config : Mapping[str, Any] | None
            Pagination configuration.

        Returns
        -------
        PaginationType | None
            The normalized pagination type, or ``None`` if unknown.
        """
        if not config:
            return None
        raw = config.get('type')
        if isinstance(raw, PaginationType):
            return raw
        if raw is None:
            return None
        try:
            return PaginationType(str(raw).strip().lower())
        except ValueError:
            return None

    @staticmethod
    def _resolve_sleep_seconds(
        explicit: float,
        rate_limit: RateLimitConfigMap | None,
    ) -> float:
        """
        Derive the effective sleep interval honoring rate-limit config.

        Parameters
        ----------
        explicit : float
            Explicit sleep seconds provided by the caller.
        rate_limit : RateLimitConfigMap | None
            Client-wide rate limit configuration.

        Returns
        -------
        float
            The resolved sleep seconds to apply between requests.
        """
        if explicit and explicit > 0:
            return explicit
        return compute_sleep_seconds(rate_limit, None)
