"""
ETLPlus API Client
======================

REST API helpers for client interactions.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any
from typing import cast
from typing import ClassVar
from typing import Literal
from typing import Mapping
from typing import NotRequired
from typing import TypedDict
from urllib.parse import parse_qsl
from urllib.parse import quote
from urllib.parse import urlencode
from urllib.parse import urlsplit
from urllib.parse import urlunsplit

from ..extract import extract as _extract


# SECTION: TYPED DICTS ====================================================== #


class PagePaginationConfig(TypedDict):
    """
    Pagination config for 'page' and 'offset' types.

    Attributes
    ----------
    type : Literal['page', 'offset']
        Pagination type.
    records_path : str
        Dotted path to records in the payload.
    max_pages : int
        Maximum number of pages to fetch.
    max_records : int
        Maximum number of records to fetch.
    page_param : str
        Query parameter name for the page number.
    size_param : str
        Query parameter name for the page size.
    start_page : int
        Starting page number (1-based).
    page_size : int
        Number of records per page.
    """
    type: Literal['page', 'offset']
    records_path: NotRequired[str]
    max_pages: NotRequired[int]
    max_records: NotRequired[int]
    page_param: NotRequired[str]
    size_param: NotRequired[str]
    start_page: NotRequired[int]
    page_size: NotRequired[int]


class CursorPaginationConfig(TypedDict):
    """
    Pagination config for cursor-based pagination.

    Attributes
    ----------
    type : Literal['cursor']
        Pagination type.
    records_path : str
        Dotted path to records in the payload.
    max_pages : int
        Maximum number of pages to fetch.
    max_records : int
        Maximum number of records to fetch.
    cursor_param : str
        Query parameter name for the cursor.
    cursor_path : str
        Dotted path to extract the next cursor from the payload.
    start_cursor : str | int
        Initial cursor value to start pagination.
    page_size : int
        Number of records per page.
    """
    type: Literal['cursor']
    records_path: NotRequired[str]
    max_pages: NotRequired[int]
    max_records: NotRequired[int]
    cursor_param: NotRequired[str]
    cursor_path: NotRequired[str]
    start_cursor: NotRequired[str | int]
    page_size: NotRequired[int]


# SECTION: TYPE ALIASES ===================================================== #


type JSONDict = dict[str, Any]
type JSONList = list[JSONDict]
type JSONData = JSONDict | JSONList

type PaginationConfig = PagePaginationConfig | CursorPaginationConfig


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
    """

    # -- Attributes -- #

    base_url: str
    endpoints: Mapping[str, str]

    # -- Class Defaults (Centralized) -- #

    DEFAULT_PAGE_PARAM: ClassVar[str] = 'page'
    DEFAULT_SIZE_PARAM: ClassVar[str] = 'per_page'
    DEFAULT_START_PAGE: ClassVar[int] = 1
    DEFAULT_PAGE_SIZE: ClassVar[int] = 100
    DEFAULT_CURSOR_PARAM: ClassVar[str] = 'cursor'
    DEFAULT_LIMIT_PARAM: ClassVar[str] = 'limit'

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

    # -- Instance Methods -- #

    def paginate(
        self,
        endpoint_key: str,
        *,
        path_parameters: dict[str, str] | None = None,
        query_parameters: dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
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
        path_parameters : dict[str, str] | None
            Values to substitute into placeholders in the endpoint path.
        query_parameters : dict[str, str] | None
            Query parameters to append (and merge with any already present on
            `base_url`).
        params : dict[str, Any] | None
            Query parameters to include in the request.
        headers : dict[str, Any] | None
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
        return self.paginate_url(
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
        params: dict[str, Any] | None,
        headers: dict[str, Any] | None,
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
        params : dict[str, Any] | None
            Query parameters to include in the request.
        headers : dict[str, Any] | None
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
        if not ptype:
            kw = EndpointClient.build_request_kwargs(
                params=params, headers=headers, timeout=timeout,
            )
            return _extract('api', url, **kw)

        records_path = pg.get('records_path')
        max_pages = pg.get('max_pages')
        max_records = pg.get('max_records')

        def _stop_limits(pages: int, recs: int) -> bool:
            if isinstance(max_pages, int) and pages >= max_pages:
                return True
            if isinstance(max_records, int) and recs >= max_records:
                return True
            return False

        results: list[dict] = []
        pages = 0
        recs = 0

        if ptype in {'page', 'offset'}:
            page_param = pg.get(
                'page_param', self.DEFAULT_PAGE_PARAM,
            )
            size_param = pg.get(
                'size_param', self.DEFAULT_SIZE_PARAM,
            )

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

            current = start_page
            while True:
                req_params = dict(params or {})
                req_params[str(page_param)] = current
                req_params[str(size_param)] = page_size
                kw = EndpointClient.build_request_kwargs(
                    params=req_params, headers=headers, timeout=timeout,
                )
                page_data = _extract('api', url, **kw)
                batch = EndpointClient.coalesce_records(
                    page_data, records_path,
                )
                results.extend(batch)
                n = len(batch)
                pages += 1
                recs += n
                if n < page_size:
                    break
                if _stop_limits(pages, recs):
                    if isinstance(max_records, int):
                        results[:] = results[: int(max_records)]
                    break
                current += 1
                EndpointClient.apply_sleep(sleep_seconds)
            return results

        if ptype == 'cursor':
            cursor_param = (
                pg.get('cursor_param', self.DEFAULT_CURSOR_PARAM)
            )
            cursor_path = cast(
                str | None,
                pg.get('cursor_path'),
            )
            try:
                page_size = int(
                    pg.get('page_size', self.DEFAULT_PAGE_SIZE),
                )
            except (TypeError, ValueError):
                page_size = self.DEFAULT_PAGE_SIZE
            page_size = 1 if page_size < 1 else page_size
            cursor_value = pg.get('start_cursor')

            def _next_cursor_from(data_obj: Any, path: str | None) -> Any:
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

            while True:
                req_params = dict(params or {})
                if cursor_value is not None:
                    req_params[str(cursor_param)] = cursor_value
                if page_size:
                    req_params.setdefault(self.DEFAULT_LIMIT_PARAM, page_size)
                kw = EndpointClient.build_request_kwargs(
                    params=req_params, headers=headers, timeout=timeout,
                )
                page_data = _extract('api', url, **kw)
                batch = EndpointClient.coalesce_records(
                    page_data, records_path,
                )
                results.extend(batch)
                n = len(batch)
                pages += 1
                recs += n

                nxt = _next_cursor_from(page_data, cursor_path)
                if not nxt or n == 0:
                    break
                if _stop_limits(pages, recs):
                    if isinstance(max_records, int):
                        results[:] = results[: int(max_records)]
                    break
                cursor_value = nxt
                EndpointClient.apply_sleep(sleep_seconds)
            return results

        # Unknown pagination type -> single request
        kw = EndpointClient.build_request_kwargs(
            params=params, headers=headers, timeout=timeout,
        )

        return _extract('api', url, **kw)

    def url(
        self,
        endpoint_key: str,
        path_parameters: dict[str, Any] | None = None,
        query_parameters: dict[str, Any] | None = None,
    ) -> str:
        """
        Build a fully qualified URL for a registered endpoint.

        Parameters
        ----------
        endpoint_key : str
            Key into the `endpoints` mapping whose relative path will be
            resolved against `base_url`.
        path_parameters : dict[str, Any], optional
            Values to substitute into placeholders in the endpoint path.
            Placeholders must be written as `{placeholder}` in the relative
            path. Each substituted value is percent-encoded as a single path
            segment (slashes are encoded) to prevent path traversal.
        query_parameters : dict[str, Any] | None, optional
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

        # Build final absolute URL.
        parts = urlsplit(self.base_url)
        base_path = parts.path.rstrip('/')
        rel_norm = '/' + rel_path.lstrip('/')
        path = (base_path + rel_norm) if base_path else rel_norm

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
        params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
        timeout: float | int | None = None,
    ) -> dict[str, Any]:
        """
        Build kwargs for requests.get.

        Only include keys that have non-empty values to keep kwargs tidy.

        Parameters
        ----------
        params : dict[str, Any] | None, optional
            Query parameters to include in the request.
        headers : dict[str, Any] | None, optional
            Headers to include in the request.
        timeout : float | int | None, optional
            Timeout for the request in seconds.

        Returns
        -------
        dict[str, Any]
            Dictionary of keyword arguments for requests.get.
        """

        kw: dict[str, Any] = {}
        if params:
            kw['params'] = params
        if headers:
            kw['headers'] = headers
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
