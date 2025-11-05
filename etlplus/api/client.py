"""
etlplus.api.client
==================

A module for registering endpoint paths, building URLs, and paginating API
responses with optional retries and rate limiting.

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

Page-based pagination
^^^^^^^^^^^^^^^^^^^^^
>>> client = EndpointClient(
...     base_url="https://api.example.com/v1",
...     endpoints={"list": "/items"},
... )
>>> pg = {"type": "page", "page_size": 100}
>>> rows = client.paginate("list", pagination=pg)

Cursor-based pagination
^^^^^^^^^^^^^^^^^^^^^^^
>>> pg = {
...   "type": "cursor",
...   "records_path": "data.items",
...   "cursor_param": "cursor",
...   "cursor_path": "data.nextCursor",
...   "page_size": 100,
... }
>>> rows = client.paginate("list", pagination=pg)
"""
from __future__ import annotations

import random
import time
from dataclasses import dataclass
from dataclasses import field
from types import MappingProxyType
from typing import Any
from typing import Callable
from typing import cast
from typing import ClassVar
from typing import Iterator
from typing import Mapping
from urllib.parse import parse_qsl
from urllib.parse import quote
from urllib.parse import urlencode
from urllib.parse import urlsplit
from urllib.parse import urlunsplit

import requests  # type: ignore

from ..extract import extract as _extract
from .errors import ApiAuthError
from .errors import ApiRequestError
from .errors import PaginationError
from .rate import compute_sleep_seconds
from .transport import build_http_adapter
from .types import HTTPAdapterMountConfig
from .types import JSONData
from .types import JSONList
from .types import PaginationConfig
from .types import RateLimitConfig
from .types import RetryPolicy


# SECTION: CLASSES ========================================================== #


@dataclass(frozen=True, slots=True)
class EndpointClient:
    """
    Immutable registry of API paths rooted at a base URL.

    Parameters
    ----------
    base_url : str
        Absolute base URL, e.g., `"https://api.example.com/v1"`
    endpoints : Mapping[str, str]
        Mapping of endpoint keys to *relative* paths, e.g.,
        `{"list_users": "/users", "user": "/users/{id}"}`.
    base_path : str | None, optional
        Optional base path to prepend to all endpoint paths. For example, if
        the base path is `/v2`, the effective endpoint path for `list_users`
        would be `/v2/users`.
    retry : RetryPolicy | None, optional
        Optional retry policy applied to HTTP requests performed by this
        client. Retries use exponential backoff with jitter to reduce
        thundering herds. If omitted, no automatic retries are performed.
    retry_network_errors : bool, optional
        When True, also retry on network errors such as timeouts and
        connection resets, in addition to HTTP status-based retries.
        Defaults to False.
    rate_limit : RateLimitConfig | None, optional
        Optional client-wide rate limit used to derive an inter-request
        delay when an explicit ``sleep_seconds`` is not provided.
    session : requests.Session | None, optional
        Optional HTTP session to use for requests. When provided, all
        requests are executed via ``session.get``.
    session_factory : Callable[[], requests.Session] | None, optional
        Optional factory to lazily create a session per client call. Ignored
        if ``session`` is provided.

    Attributes
    ----------
    base_url : str
        The absolute base URL used as the root for all endpoints (e.g.,
        `"https://api.example.com/v1"`).
    endpoints : Mapping[str, str]
        Mapping of endpoint keys to *relative* paths (read-only), e.g.,
        `{"list_users": "/users", "user": "/users/{id}"}`. A defensive copy of
        the mapping supplied at construction. The dataclass is frozen
        (attributes are read-only), and the mapping is wrapped in a
        read-only proxy to prevent mutation.
    retry : RetryPolicy | None
        Optional retry policy. When set, requests are retried on configured
        HTTP status codes with exponential backoff and jitter.
    retry_network_errors : bool
        When True, also retry on common network failures (timeouts,
        connection errors).
    rate_limit : RateLimitConfig | None
        Optional rate limiting configuration used to compute an inter-request
        sleep when not explicitly provided to ``paginate``/``paginate_url``.
    session : Any | None
        Optional HTTP session used for API requests.
    session_factory : Callable[[], Any] | None
        Optional factory used to create a session when needed.

    Raises
    ------
    ValueError
        If `base_url` is not absolute or if any endpoint key/value is not a
        non-empty `str`.

    Examples
    --------
    >>> ep = Endpoint(
    ...     base_url="https://api.example.com/v1",
    ...     endpoints={"list_users": "users", "user": "/users/{id}"}
    ... )
    >>> ep.url("list_users", {"active": "true"})
    'https://api.example.com/v1/users?active=true'

    Configure retries with jitter and (optionally) network error retries:

    >>> client = EndpointClient(
    ...     base_url="https://api.example.com/v1",
    ...     endpoints={"list": "/items"},
    ...     retry={"max_attempts": 5, "backoff": 0.5, "retry_on": [429, 503]},
    ...     retry_network_errors=True,
    ... )
    >>> client.paginate("list", pagination={"type": "page", "page_size": 50})
    """

    # -- Attributes -- #

    base_url: str
    endpoints: Mapping[str, str]
    base_path: str | None = None

    # Optional retry configuration (constructor parameter; object is frozen)
    retry: RetryPolicy | None = None
    retry_network_errors: bool = False
    # Optional client-wide rate limit configuration
    rate_limit: RateLimitConfig | None = None

    # Optional HTTP session or factory
    session: Any | None = None
    session_factory: Callable[[], Any] | None = None

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
        # Validate base_url is absolute.
        parts = urlsplit(self.base_url)
        if not parts.scheme or not parts.netloc:
            raise ValueError(
                'base_url must be absolute, e.g. "https://api.example.com"',
            )

        # Defensive copy + validate endpoints.
        eps = dict(self.endpoints)
        for k, v in eps.items():
            if not isinstance(k, str) or not isinstance(v, str) or not v:
                raise ValueError(
                    'endpoints must map str -> non-empty str',
                )
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

    def __enter__(self) -> EndpointClient:
        """Enter a context where a session is managed by the client.

        Behavior
        --------
        - If an explicit ``session`` was provided at construction, it is used
          and will NOT be closed on exit.
        - Else if a ``session_factory`` exists, it is used to create a session
          which WILL be closed on exit.
        - Else a default ``requests.Session`` is created and WILL be closed on
          exit.
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
        exc_type,
        exc,
        tb,
    ) -> None:
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
            Keyword arguments forwarded to ``requests.get`` via ``extract``.

        Returns
        -------
        JSONData
            The parsed JSON payload or a fallback object when appropriate.

        Raises
        ------
        requests.RequestException
            If all retry attempts fail with a retry-eligible HTTP status
            (or if no policy is configured and the request fails).
        ValueError
            Propagated if JSON parsing fails within ``extract``.
        """

        # Determine session to use for this request.
        # Prefer context-managed session when present
        sess = (
            self._ctx_session
            if self._ctx_session is not None
            else self.session
        )
        if sess is None and self.session_factory is not None:
            sess = self.session_factory()

        policy: RetryPolicy | None = self.retry
        if not policy:
            if sess is not None:
                kw = dict(kw)
                kw['session'] = sess
            try:
                return _extract('api', url, **kw)
            except requests.RequestException as e:  # pragma: no cover (net)
                status = getattr(
                    getattr(e, 'response', None), 'status_code', None,
                )
                retried = False
                err: ApiRequestError
                if status in {401, 403}:
                    err = ApiAuthError(
                        url=url,
                        status=status,
                        attempts=1,
                        retried=retried,
                        retry_policy=None,
                        cause=e,
                    )
                else:
                    err = ApiRequestError(
                        url=url,
                        status=status,
                        attempts=1,
                        retried=retried,
                        retry_policy=None,
                        cause=e,
                    )
                raise err from e

        max_attempts = int(
            cast(int | float | str | None, policy.get('max_attempts', 0))
            or self.DEFAULT_RETRY_MAX_ATTEMPTS,
        )
        if max_attempts < 1:
            max_attempts = 1

        try:
            backoff = float(policy.get('backoff', self.DEFAULT_RETRY_BACKOFF))
        except (TypeError, ValueError):
            backoff = self.DEFAULT_RETRY_BACKOFF
        backoff = 0.0 if backoff < 0 else backoff

        retry_on = policy.get('retry_on')
        if not retry_on:
            retry_on_codes = set(self.DEFAULT_RETRY_ON)
        else:
            retry_on_codes = {int(c) for c in retry_on}

        attempt = 1
        cap = self.DEFAULT_RETRY_CAP
        while True:
            try:
                if sess is not None:
                    call_kw = dict(kw)
                    call_kw['session'] = sess
                else:
                    call_kw = kw
                return _extract('api', url, **call_kw)
            except requests.RequestException as e:  # pragma: no cover (net)
                status = getattr(
                    getattr(e, 'response', None), 'status_code', None,
                )
                # HTTP status-based retry
                should_retry = (
                    isinstance(status, int) and status in retry_on_codes
                )

                # Network error retry (timeouts/connection failures)
                if not should_retry and self.retry_network_errors:
                    is_timeout = isinstance(e, requests.Timeout)
                    is_conn = isinstance(e, requests.ConnectionError)
                    should_retry = is_timeout or is_conn
                if not should_retry or attempt >= max_attempts:
                    retried = attempt > 1
                    final_err: ApiRequestError
                    if status in {401, 403}:
                        final_err = ApiAuthError(
                            url=url,
                            status=status,
                            attempts=attempt,
                            retried=retried,
                            retry_policy=policy,
                            cause=e,
                        )
                    else:
                        final_err = ApiRequestError(
                            url=url,
                            status=status,
                            attempts=attempt,
                            retried=retried,
                            retry_policy=policy,
                            cause=e,
                        )
                    raise final_err from e

                # Exponential backoff with Full Jitter to reduce herding:
                #   sleep = random.uniform(0, min(cap, backoff * 2**(n-1)))
                if backoff > 0:
                    exp = backoff * (2 ** (attempt - 1))
                    upper = exp if exp < cap else cap
                    sleep = random.uniform(0.0, upper)
                else:
                    sleep = 0.0
                EndpointClient.apply_sleep(sleep)
                attempt += 1

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
        Convenience wrapper to paginate by endpoint key.

        Builds the URL via `self.url(...)` and delegates to `paginate_url`.

        Parameters
        ----------
        endpoint_key : str
            Key into the `endpoints` mapping whose relative path will be
            resolved against `base_url`.
        path_parameters : Mapping[str, str] | None
            Values to substitute into placeholders in the endpoint path.
        query_parameters : Mapping[str, str] | None
            Query parameters to append (and merge with any already present on
            `base_url`).
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

        This is a generator variant of ``paginate`` that yields record dicts
        across pages instead of aggregating them into a list. Only used when a
        pagination configuration is provided.

        Parameters
        ----------
        endpoint_key : str
            Key into the ``endpoints`` mapping whose relative path will be
            resolved against ``base_url``.
        path_parameters, query_parameters, params, headers, timeout,
        pagination, sleep_seconds
            See ``paginate`` for parameter semantics.

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
        params, headers, timeout, pagination, sleep_seconds
            See ``paginate_url`` for parameter semantics.

        Yields
        ------
        dict
            Record dictionaries extracted from each page.
        """

        # Normalize pagination config for typed access.
        pg: dict[str, Any] = cast(dict[str, Any], pagination or {})
        ptype = pg.get('type') if pagination else None

        # Determine effective sleep seconds: explicit parameter wins; otherwise
        # compute from client rate_limit, if any.
        effective_sleep = (
            sleep_seconds
            if (sleep_seconds and sleep_seconds > 0)
            else compute_sleep_seconds(self.rate_limit, None)
        )

        # Helper for stop limits.
        max_pages = pg.get('max_pages')
        max_records = pg.get('max_records')

        def _stop_limits(pages: int, recs: int) -> bool:
            if isinstance(max_pages, int) and pages >= max_pages:
                return True
            if isinstance(max_records, int) and recs >= max_records:
                return True
            return False

        # Known pagination strategies.
        if ptype in {'page', 'offset'}:
            page_param = pg.get('page_param', self.DEFAULT_PAGE_PARAM)
            size_param = pg.get('size_param', self.DEFAULT_SIZE_PARAM)

            start_page = int(
                cast(
                    int | float | str,
                    pg.get('start_page', self.DEFAULT_START_PAGE),
                ),
            )
            start_page = 1 if start_page < 1 else start_page

            page_size = int(
                cast(
                    int | float | str,
                    pg.get('page_size', self.DEFAULT_PAGE_SIZE),
                ),
            )
            page_size = 1 if page_size < 1 else page_size

            records_path = pg.get('records_path')
            pages = 0
            recs = 0
            current = start_page
            while True:
                req_params = dict(params or {})
                req_params[str(page_param)] = current
                req_params[str(size_param)] = page_size
                kw = EndpointClient.build_request_kwargs(
                    params=req_params, headers=headers, timeout=timeout,
                )
                try:
                    page_data = self._extract_with_retry(url, **kw)
                except ApiRequestError as e:
                    raise PaginationError(
                        url=url,
                        status=e.status,
                        attempts=e.attempts,
                        retried=e.retried,
                        retry_policy=e.retry_policy,
                        cause=e,
                        page=current,
                    ) from e
                batch = EndpointClient.coalesce_records(
                    page_data, records_path,
                )
                n = len(batch)
                pages += 1
                recs += n

                # Yield with respect to max_records cap.
                if isinstance(max_records, int) and recs > max_records:
                    take = max(0, int(max_records) - (recs - n))
                    yield from batch[:take]
                    break
                yield from batch

                if n < page_size:
                    break
                if _stop_limits(pages, recs):
                    break
                current += 1
                EndpointClient.apply_sleep(effective_sleep)
            return

        if ptype == 'cursor':
            cursor_param = pg.get('cursor_param', self.DEFAULT_CURSOR_PARAM)
            cursor_path = cast(str | None, pg.get('cursor_path'))
            try:
                page_size = int(pg.get('page_size', self.DEFAULT_PAGE_SIZE))
            except (TypeError, ValueError):
                page_size = self.DEFAULT_PAGE_SIZE
            page_size = 1 if page_size < 1 else page_size
            cursor_value = pg.get('start_cursor')

            records_path = pg.get('records_path')
            pages = 0
            recs = 0
            while True:
                req_params = dict(params or {})
                if cursor_value is not None:
                    req_params[str(cursor_param)] = cursor_value
                if page_size:
                    req_params.setdefault(self.DEFAULT_LIMIT_PARAM, page_size)
                kw = EndpointClient.build_request_kwargs(
                    params=req_params, headers=headers, timeout=timeout,
                )
                try:
                    page_data = self._extract_with_retry(url, **kw)
                except ApiRequestError as e:
                    raise PaginationError(
                        url=url,
                        status=e.status,
                        attempts=e.attempts,
                        retried=e.retried,
                        retry_policy=e.retry_policy,
                        cause=e,
                        page=pages + 1,
                    ) from e
                batch = EndpointClient.coalesce_records(
                    page_data, records_path,
                )
                n = len(batch)
                pages += 1
                recs += n

                if isinstance(max_records, int) and recs > max_records:
                    take = max(0, int(max_records) - (recs - n))
                    yield from batch[:take]
                    break
                yield from batch

                nxt = EndpointClient.next_cursor_from(page_data, cursor_path)
                if not nxt or n == 0:
                    break
                if _stop_limits(pages, recs):
                    break
                cursor_value = nxt
                EndpointClient.apply_sleep(effective_sleep)
            return

        # No/unknown pagination type: single request, coalesce, and yield.
        kw = EndpointClient.build_request_kwargs(
            params=params, headers=headers, timeout=timeout,
        )
        page_data = self._extract_with_retry(url, **kw)
        records_path = pg.get('records_path')
        yield from EndpointClient.coalesce_records(page_data, records_path)

    def url(
        self,
        endpoint_key: str,
        path_parameters: Mapping[str, Any] | None = None,
        query_parameters: Mapping[str, Any] | None = None,
    ) -> str:
        """
        Build a fully qualified URL for a registered endpoint.

        Parameters
        ----------
        endpoint_key : str
            Key into the `endpoints` mapping whose relative path will be
            resolved against `base_url`.
        path_parameters : Mapping[str, Any] | None, optional
            Values to substitute into placeholders in the endpoint path.
            Placeholders must be written as `{placeholder}` in the relative
            path. Each substituted value is percent-encoded as a single path
            segment (slashes are encoded) to prevent path traversal.
        query_parameters : Mapping[str, Any] | None, optional
            Query parameters to append (and merge with any already present on
            `base_url`). Values are percent-encoded and combined using
            `application/x-www-form-urlencoded` rules.

        Returns
        -------
        str
            The constructed absolute URL.

        Raises
        ------
        KeyError
            If `endpoint_key` is unknown or a required `{placeholder}`
            in the path has no corresponding entry in `path_parameters`.

        Examples
        --------
        >>> ep = Endpoint(
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
        sleeper=None,
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

        def _get_path(obj: Any, path: str) -> Any:
            cur = obj
            for part in path.split('.'):
                if isinstance(cur, dict):
                    cur = cur.get(part)
                else:
                    return None
            return cur

        data = x
        if isinstance(records_path, str) and records_path:
            data = _get_path(x, records_path)

        if isinstance(data, list):
            out: list[dict] = []
            for item in data:
                if isinstance(item, dict):
                    out.append(item)
                else:
                    out.append({'value': item})
            return out
        if isinstance(data, dict):
            items = data.get('items')
            if isinstance(items, list):
                return EndpointClient.coalesce_records(items, None)
            return [data]

        return [{'value': data}]

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

        if not (
            isinstance(path, str)
            and path
            and isinstance(data_obj, dict)
        ):
            return None
        cur: Any = data_obj
        for part in path.split('.'):
            if isinstance(cur, dict):
                cur = cur.get(part)
            else:
                return None

        return cur if isinstance(cur, (str, int)) else None
