"""
:mod:`etlplus.api.response` module.

Centralized logic for handling REST API endpoint responses, including:
- Pagination strategies (page, offset, cursor).
- Record extraction from JSON payloads.

This module provides a :class:`Paginator` class that encapsulates pagination
configuration and behavior. It supports instantiation from a configuration
mapping, fetching pages via a supplied callback, and iterating over records
across pages.

Examples
--------
Create a paginator from config and use it to fetch all records from an API
endpoint:
>>> cfg = {
...     "type": "page",
...     "page_param": "page",
...     "size_param": "per_page",
...     "start_page": 1,
...     "page_size": 100,
...     "records_path": "data.items",
...     "max_pages": 10,
... }
>>> def fetch(url, params, page):
...     response = requests.get(url, params=params)
...     response.raise_for_status()
...     return response.json()
>>> paginator = Paginator.from_config(
...     cfg,
...     fetch=fetch,
...     rate_limiter=RateLimiter.fixed(0.5),
... )
>>> all_records = paginator.paginate('https://api.example.com/v1/items')
"""
from __future__ import annotations

from collections.abc import Callable
from collections.abc import Iterator
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any
from typing import ClassVar
from typing import Literal
from typing import Required
from typing import TypedDict
from typing import cast

from ..types import JSONDict
from ..types import JSONRecords
from ..utils import to_int
from ..utils import to_maximum_int
from ..utils import to_positive_int
from .errors import ApiRequestError
from .errors import PaginationError
from .request import RateLimiter

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'Paginator',

    # Enums
    'PaginationType',

    # Typed Dicts
    'CursorPaginationConfigMap',
    'PagePaginationConfigMap',
    'PaginationConfigMap',
]


# SECTION: CONSTANTS ======================================================== #


_MISSING = object()


# SECTION: ENUMS ============================================================ #


class PaginationType(StrEnum):
    """Enumeration of supported pagination types for REST API responses."""

    PAGE = 'page'
    OFFSET = 'offset'
    CURSOR = 'cursor'


# SECTION: TYPED DICTS ====================================================== #


class CursorPaginationConfigMap(TypedDict, total=False):
    """
    Configuration mapping for cursor-based REST API response pagination.

    Supports fetching successive result pages using a cursor token returned in
    each response. Values are all optional except ``type``.

    Attributes
    ----------
    type : Required[Literal[PaginationType.CURSOR]]
        Pagination type discriminator.
    records_path : str
        Dotted path to the records list in each page payload.
    fallback_path : str
        Secondary dotted path consulted when ``records_path`` resolves to an
        empty collection or ``None``.
    max_pages : int
        Maximum number of pages to fetch.
    max_records : int
        Maximum number of records to fetch across all pages.
    cursor_param : str
        Query parameter name carrying the cursor value.
    cursor_path : str
        Dotted path inside the payload to the next cursor.
    start_cursor : str | int
        Initial cursor value used for the first request.
    page_size : int
        Number of records per page.
    limit_param : str
        Query parameter name carrying the page size for cursor-based
        pagination when the API uses a separate limit field.

    Examples
    --------
    >>> cfg: CursorPaginationConfig = {
    ...     'type': 'cursor',
    ...     'records_path': 'data.items',
    ...     'cursor_param': 'cursor',
    ...     'cursor_path': 'data.nextCursor',
    ...     'page_size': 100,
    ... }
    """

    # -- Attributes -- #

    type: Required[Literal[PaginationType.CURSOR]]
    records_path: str
    fallback_path: str
    max_pages: int
    max_records: int
    cursor_param: str
    cursor_path: str
    start_cursor: str | int
    page_size: int
    limit_param: str


class PagePaginationConfigMap(TypedDict, total=False):
    """
    Configuration mapping for page-based and offset-based REST API response
    pagination.

    Controls page-number or offset-based pagination. Values are optional
    except ``type``.

    Attributes
    ----------
    type : Required[Literal[PaginationType.PAGE, PaginationType.OFFSET]]
        Pagination type discriminator.
    records_path : str
        Dotted path to the records list in each page payload.
    fallback_path : str
        Secondary dotted path consulted when ``records_path`` resolves to an
        empty collection or ``None``.
    max_pages : int
        Maximum number of pages to fetch.
    max_records : int
        Maximum number of records to fetch across all pages.
    page_param : str
        Query parameter name carrying the page number or offset.
    size_param : str
        Query parameter name carrying the page size.
    start_page : int
        Starting page number or offset (1-based).
    page_size : int
        Number of records per page.

    Examples
    --------
    >>> cfg: PagePaginationConfig = {
    ...     'type': 'page',
    ...     'records_path': 'data.items',
    ...     'page_param': 'page',
    ...     'size_param': 'per_page',
    ...     'start_page': 1,
    ...     'page_size': 100,
    ... }
    """

    # -- Attributes -- #

    type: Required[Literal[PaginationType.PAGE, PaginationType.OFFSET]]
    records_path: str
    fallback_path: str
    max_pages: int
    max_records: int
    page_param: str
    size_param: str
    start_page: int
    page_size: int


# SECTION: TYPE ALIASES ===================================================== #


type FetchPageFunc = Callable[
    [str, Mapping[str, Any] | None, int | None], Any,
]

type PaginationConfigMap = PagePaginationConfigMap | CursorPaginationConfigMap


# SECTION: CLASSES ========================================================== #


@dataclass(slots=True, kw_only=True)
class Paginator:
    """
    REST API endpoint response pagination manager.

    The caller supplies a ``fetch`` function that retrieves a JSON page
    given an absolute URL and request params.  The paginator handles iterating
    over pages according to the configured strategy, extracting records from
    each page, and yielding them one by one.  Pagination strategies supported
    are:
    - Cursor/token based (``type='cursor'``)
    - Offset based (``type='offset'``)
    - Page-number based (``type='page'``)

    Attributes
    ----------
    START_PAGE : ClassVar[int]
        Default starting page number.
    PAGE_SIZE : ClassVar[int]
        Default number of records per page.
    CURSOR_PARAM : ClassVar[str]
        Default query parameter name for cursor value.
    LIMIT_PARAM : ClassVar[str]
        Default query parameter name for page size in cursor pagination.
    PAGE_PARAMS : ClassVar[dict[PaginationType, str]]
        Default query parameter name for page number per pagination type.
    SIZE_PARAMS : ClassVar[dict[PaginationType, str]]
        Default query parameter name for page size per pagination type.
    START_PAGES : ClassVar[dict[PaginationType, int]]
        Default starting page number per pagination type.
    type : PaginationType
        Pagination type: ``"page"``, ``"offset"``, or ``"cursor"``.
    page_size : int
        Number of records per page (minimum of 1).
    start_page : int
        Starting page number or offset, depending on ``type``.
    start_cursor : object | None
        Initial cursor value for cursor-based pagination.
    records_path : str | None
        Dotted path to the records list inside each page payload.
    fallback_path : str | None
        Alternate dotted path used when ``records_path`` resolves to an empty
        collection or ``None``.
    cursor_path : str | None
        Dotted path to the next-cursor value inside each page payload.
    max_pages : int | None
        Optional maximum number of pages to fetch.
    max_records : int | None
        Optional maximum number of records to fetch.
    page_param : str
        Query parameter name carrying the page number or offset.
    size_param : str
        Query parameter name carrying the page size.
    cursor_param : str
        Query parameter name carrying the cursor.
    limit_param : str
        Query parameter name carrying the page size for cursor-based
        pagination when the API uses a separate limit field.
    fetch : FetchPageFunc | None
        Callback used to fetch a single page. It receives the absolute URL,
        the request params mapping, and the 1-based page index.
    rate_limiter : RateLimiter | None
        Optional rate limiter invoked between page fetches.
    last_page : int
        Tracks the last page index attempted. Useful for diagnostics.
    """

    # -- Constants -- #

    # Pagination defaults
    START_PAGE: ClassVar[int] = 1
    PAGE_SIZE: ClassVar[int] = 100
    CURSOR_PARAM: ClassVar[str] = PaginationType.CURSOR
    LIMIT_PARAM: ClassVar[str] = 'limit'

    # Mapped pagination defaults
    PAGE_PARAMS: ClassVar[dict[PaginationType, str]] = {
        PaginationType.PAGE: 'page',
        PaginationType.OFFSET: 'offset',
        PaginationType.CURSOR: 'page',
    }
    SIZE_PARAMS: ClassVar[dict[PaginationType, str]] = {
        PaginationType.PAGE: 'per_page',
        PaginationType.OFFSET: 'limit',
        PaginationType.CURSOR: 'limit',
    }
    START_PAGES: ClassVar[dict[PaginationType, int]] = {
        PaginationType.PAGE: 1,
        PaginationType.OFFSET: 0,
        PaginationType.CURSOR: 1,
    }

    # -- Attributes -- #

    type: PaginationType = PaginationType.PAGE
    page_size: int = PAGE_SIZE
    start_page: int = START_PAGE
    # start_cursor: str | int | None = None
    start_cursor: object | None = None
    records_path: str | None = None
    fallback_path: str | None = None
    cursor_path: str | None = None
    max_pages: int | None = None
    max_records: int | None = None
    page_param: str = ''
    size_param: str = ''
    cursor_param: str = ''
    limit_param: str = ''

    # -- Magic Methods (Object Lifecycle) -- #

    def __post_init__(self):
        # Normalize type to supported PaginationType.
        if self.type not in (
            PaginationType.PAGE,
            PaginationType.OFFSET,
            PaginationType.CURSOR,
        ):
            self.type = PaginationType.PAGE
        # Normalize start_page based on type.
        if self.start_page < 0:
            self.start_page = self.START_PAGES[self.type]
        if self.type == PaginationType.PAGE and self.start_page < 1:
            self.start_page = 1
        # Enforce minimum page_size.
        if self.page_size < 1:
            self.page_size = 1
        # Normalize parameter names by type-specific defaults.
        if not self.page_param:
            self.page_param = self.PAGE_PARAMS[self.type]
        if not self.size_param:
            self.size_param = self.SIZE_PARAMS[self.type]
        if not self.cursor_param:
            self.cursor_param = self.CURSOR_PARAM
        if not self.limit_param:
            self.limit_param = self.LIMIT_PARAM

    fetch: FetchPageFunc | None = None
    rate_limiter: RateLimiter | None = None
    last_page: int = 0

    # -- Class Methods -- #

    @classmethod
    def from_config(
        cls,
        config: Mapping[str, Any],
        *,
        fetch: FetchPageFunc,
        rate_limiter: RateLimiter | None = None,
    ) -> Paginator:
        """
        Normalize config and build a paginator instance.

        Parameters
        ----------
        config : Mapping[str, Any]
            Pagination configuration mapping.
        fetch : FetchPageFunc
            Callback used to fetch a single page for a request given the
            absolute URL, the request params mapping, and the 1-based page
            index.
        rate_limiter : RateLimiter | None, optional
            Optional limiter invoked between page fetches.

        Returns
        -------
        Paginator
            Configured paginator instance.
        """
        ptype_raw = str(config.get('type', 'page')).strip().lower()
        try:
            ptype = PaginationType(ptype_raw)
        except ValueError:
            ptype = PaginationType.PAGE

        return cls(
            type=ptype,
            page_size=to_positive_int(config.get('page_size'), cls.PAGE_SIZE),
            start_page=to_maximum_int(
                config.get('start_page'),
                cls.START_PAGES[ptype],
            ),
            start_cursor=config.get('start_cursor'),
            records_path=config.get('records_path'),
            fallback_path=config.get('fallback_path'),
            # cursor_path=config.get('cursor_path'),
            cursor_path=str(config.get('cursor_path', '')) or None,
            max_pages=to_int(config.get('max_pages'), None, minimum=1),
            max_records=to_int(config.get('max_records'), None, minimum=1),
            page_param=str(config.get('page_param', '')),
            size_param=str(config.get('size_param', '')),
            cursor_param=str(config.get('cursor_param', '')),
            limit_param=str(config.get('limit_param', '')),
            fetch=fetch,
            rate_limiter=rate_limiter,
        )

    # Instance Methods -- #

    def paginate(
        self,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
    ) -> JSONRecords:
        """Collect all records across pages into a list of dicts."""
        return list(self.paginate_iter(url, params=params))

    def paginate_iter(
        self,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
    ) -> Iterator[JSONDict]:
        """Yield record dicts across pages for the configured strategy."""
        if self.fetch is None:
            raise ValueError('Paginator.fetch must be provided')

        pages = 0
        recs = 0

        if self.type in (PaginationType.PAGE, PaginationType.OFFSET):
            current = self.start_page
            while True:
                self.last_page = pages + 1
                req_params = dict(params or {})
                req_params[self.page_param] = current
                req_params[self.size_param] = self.page_size

                page_data = self._fetch_page(
                    url,
                    req_params,
                )
                batch = self.coalesce_records(
                    page_data,
                    self.records_path,
                    self.fallback_path,
                )
                n = len(batch)
                pages += 1
                recs += n

                if (
                    isinstance(self.max_records, int)
                    and recs > self.max_records
                ):
                    take = max(
                        0,
                        self.max_records - (recs - n),
                    )
                    yield from batch[:take]
                    break

                yield from batch

                if n < self.page_size:
                    break
                if self._stop_limits(pages, recs):
                    break

                if self.type == PaginationType.PAGE:
                    current += 1
                else:
                    current += self.page_size

                self._enforce_rate_limit()
            return

        if self.type == PaginationType.CURSOR:
            cursor = self.start_cursor
            while True:
                self.last_page = pages + 1
                req_params = dict(params or {})
                if cursor is not None:
                    req_params[self.cursor_param] = cursor
                req_params.setdefault(self.limit_param, self.page_size)

                page_data = self._fetch_page(
                    url,
                    req_params,
                )
                batch = self.coalesce_records(
                    page_data,
                    self.records_path,
                    self.fallback_path,
                )
                n = len(batch)
                pages += 1
                recs += n

                if (
                    isinstance(self.max_records, int)
                    and recs > self.max_records
                ):
                    take = max(
                        0,
                        self.max_records - (recs - n),
                    )
                    yield from batch[:take]
                    break

                yield from batch

                nxt = self.next_cursor_from(
                    page_data,
                    self.cursor_path,
                )
                if not nxt or n == 0:
                    break
                if self._stop_limits(pages, recs):
                    break
                cursor = nxt
                self._enforce_rate_limit()
            return

        # Fallback: single page, coalesce. and yield.
        self.last_page = 1
        page_data = self._fetch_page(url, params)
        yield from self.coalesce_records(
            page_data,
            self.records_path,
            self.fallback_path,
        )

    # -- Protected Instance Methods -- #

    def _fetch_page(
        self,
        url: str,
        params: Mapping[str, Any] | None,
    ) -> Any:
        """
        Fetch a single page and attach page index on failure.

        When the underlying ``fetch`` raises :class:`ApiRequestError`, this
        helper re-raises :class:`PaginationError` with the current
        ``last_page`` value populated so callers can inspect the failing
        page index.

        Parameters
        ----------
        url : str
            Absolute URL of the endpoint to fetch.
        params : Mapping[str, Any] | None
            Optional query parameters for the request.

        Returns
        -------
        Any
            Parsed JSON payload of the fetched page.

        Raises
        ------
        PaginationError
            When the underlying ``fetch`` fails with :class:`ApiRequestError`.
        ValueError
            When ``fetch`` is not provided.
        """
        if self.fetch is None:
            raise ValueError('Paginator.fetch must be provided')
        try:
            return self.fetch(url, params, self.last_page)
        except ApiRequestError as e:
            raise PaginationError(
                url=e.url,
                status=e.status,
                attempts=e.attempts,
                retried=e.retried,
                retry_policy=e.retry_policy,
                cause=e,
                page=self.last_page,
            ) from e

    def _stop_limits(
        self, pages: int,
        recs: int,
    ) -> bool:
        """
        Check if pagination limits have been reached.

        Parameters
        ----------
        pages : int
            Number of pages fetched so far.
        recs : int
            Number of records fetched so far.

        Returns
        -------
        bool
            True if any limit has been reached, False otherwise.
        """
        if isinstance(self.max_pages, int) and pages >= self.max_pages:
            return True
        if isinstance(self.max_records, int) and recs >= self.max_records:
            return True
        return False

    def _enforce_rate_limit(self) -> None:
        """Apply configured pacing between subsequent page fetches."""
        if self.rate_limiter is not None:
            self.rate_limiter.enforce()

    # -- Static Methods -- #

    @staticmethod
    def coalesce_records(
        x: Any,
        records_path: str | None,
        fallback_path: str | None = None,
    ) -> JSONRecords:
        """
        Coalesce JSON page payloads into a list of dicts.

        Parameters
        ----------
        x : Any
            The JSON payload from an API response.
        records_path : str | None
            Optional dotted path to the records within the payload.
        fallback_path : str | None
            Secondary dotted path consulted when ``records_path`` resolves to
            ``None`` or an empty list.

        Returns
        -------
        JSONRecords
            List of record dicts extracted from the payload.

        Notes
        -----
        Supports dotted path extraction via ``records_path`` and handles
        lists, mappings, and scalars by coercing non-dict items into
        ``{"value": x}``.
        """
        def _resolve(obj: Any, path: str | None) -> Any:
            if not isinstance(path, str) or not path:
                return obj
            cur: Any = obj
            for part in path.split('.'):
                if isinstance(cur, dict) and part in cur:
                    cur = cur[part]
                else:
                    return _MISSING
            return cur

        data = _resolve(x, records_path)
        if data is _MISSING:
            data = None

        if fallback_path and (
            data is None
            or (isinstance(data, list) and not data)
        ):
            fallback = _resolve(x, fallback_path)
            if fallback is not _MISSING:
                data = fallback

        if data is None and not records_path:
            data = x

        if isinstance(data, list):
            out: JSONRecords = []
            for item in data:
                if isinstance(item, dict):
                    out.append(cast(JSONDict, item))
                else:
                    out.append(cast(JSONDict, {'value': item}))
            return out
        if isinstance(data, dict):
            items = data.get('items')
            if isinstance(items, list):
                return Paginator.coalesce_records(items, None)
            return [cast(JSONDict, data)]

        return [cast(JSONDict, {'value': data})]

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
            The JSON payload object (expected to be a mapping).
        path : str | None
            Dotted path within the payload that points to the next cursor.

        Returns
        -------
        str | int | None
            The extracted cursor value if present and of type ``str`` or
            ``int``; otherwise ``None``.
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
