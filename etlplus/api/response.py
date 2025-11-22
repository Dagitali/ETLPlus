"""
``etlplus.api.response`` module.

Centralized logic for handling REST API endpoint responses, including:
- Pagination strategies (page, offset, cursor)
- Record extraction from JSON payloads

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
...     sleep_func=time.sleep,
...     sleep_seconds=0.5,
... )
>>> all_records = paginator.paginate("https://api.example.com/v1/items")
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from typing import Callable
from typing import cast
from typing import ClassVar
from typing import Iterator
from typing import Literal
from typing import Mapping
from typing import NotRequired
from typing import TypedDict

from .errors import ApiRequestError
from .errors import PaginationError


# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'PaginationConfig',
    'Paginator',
]


# SECTION: TYPED DICTS ====================================================== #


class CursorPaginationConfig(TypedDict):
    """
    Configuration for cursor-based pagination.

    Supports fetching successive result pages using a cursor token returned in
    each response. Values are all optional except ``type``.

    Attributes
    ----------
    type : Literal['cursor']
        Pagination type discriminator.
    records_path : NotRequired[str]
        Dotted path to the records list in each page payload.
    max_pages : NotRequired[int]
        Maximum number of pages to fetch.
    max_records : NotRequired[int]
        Maximum number of records to fetch across all pages.
    cursor_param : NotRequired[str]
        Query parameter name carrying the cursor value.
    cursor_path : NotRequired[str]
        Dotted path inside the payload pointing to the next cursor.
    start_cursor : NotRequired[str | int]
        Initial cursor value used for the first request.
    page_size : NotRequired[int]
        Number of records per page.

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

    type: Literal['cursor']
    records_path: NotRequired[str]
    max_pages: NotRequired[int]
    max_records: NotRequired[int]
    cursor_param: NotRequired[str]
    cursor_path: NotRequired[str]
    start_cursor: NotRequired[str | int]
    page_size: NotRequired[int]


class PagePaginationConfig(TypedDict):
    """
    Configuration for 'page' and 'offset' pagination types.

    Controls page-number or offset-based pagination. Values are optional
    except ``type``.

    Attributes
    ----------
    type : Literal['page', 'offset']
        Pagination type discriminator.
    records_path : NotRequired[str]
        Dotted path to the records list in each page payload.
    max_pages : NotRequired[int]
        Maximum number of pages to fetch.
    max_records : NotRequired[int]
        Maximum number of records to fetch across all pages.
    page_param : NotRequired[str]
        Query parameter name carrying the page number.
    size_param : NotRequired[str]
        Query parameter name carrying the page size.
    start_page : NotRequired[int]
        Starting page number (1-based).
    page_size : NotRequired[int]
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

    type: Literal['page', 'offset']
    records_path: NotRequired[str]
    max_pages: NotRequired[int]
    max_records: NotRequired[int]
    page_param: NotRequired[str]
    size_param: NotRequired[str]
    start_page: NotRequired[int]
    page_size: NotRequired[int]


class PaginationConfig(TypedDict, total=False):
    """
    Configuration for REST API endpoint pagination.

    Combines settings for page/offset and cursor-based pagination. All fields
    are optional.

    Attributes
    ----------
    type : str
        Pagination type: 'page', 'offset', or 'cursor'.
    page_size : int
        Number of records per page.
    start_page : int
        Starting page number or offset.
    start_cursor : str | None
        Initial cursor value for cursor-based pagination.
    records_path : str | None
        Dotted path to the records list inside each page payload.
    cursor_path : str | None
        Dotted path to the next-cursor value inside each page payload.
    max_pages : int | None
        Optional maximum number of pages to fetch.
    max_records : int | None
        Optional maximum number of records to fetch.
    page_param : str | None
        Query parameter name carrying the page number or offset.
    size_param : str | None
        Query parameter name carrying the page size.
    cursor_param : str | None
        Query parameter name carrying the cursor.
    limit_param : str | None
        Query parameter name carrying the page size for cursor-based
        pagination when the API uses a separate limit field.
    """

    type: str  # 'page', 'offset', 'cursor'
    page_size: int
    start_page: int
    start_cursor: str | None
    records_path: str | None
    cursor_path: str | None
    max_pages: int | None
    max_records: int | None
    page_param: str | None
    size_param: str | None
    cursor_param: str | None
    limit_param: str | None


# SECTION: TYPE ALIASES ===================================================== #


# Literal type for supported pagination kinds
type PaginationType = Literal['page', 'offset', 'cursor']


# SECTION: CLASSES ========================================================== #


@dataclass(slots=True)
class Paginator:
    """
    REST API endpoint response pagination engine.

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
    PAGE_PARAM : ClassVar[str]
        Default query parameter name for page number.
    SIZE_PARAM : ClassVar[str]
        Default query parameter name for page size.
    START_PAGE : ClassVar[int]
        Default starting page number.
    PAGE_SIZE : ClassVar[int]
        Default number of records per page.
    CURSOR_PARAM : ClassVar[str]
        Default query parameter name for cursor value.
    LIMIT_PARAM : ClassVar[str]
        Default query parameter name for page size in cursor pagination.
    type : PaginationType
        Pagination type: ``"page"``, ``"offset"``, or ``"cursor"``.
    page_size : int
        Number of records per page (minimum of 1).
    start_page : int
        Starting page number or offset, depending on ``type``.
    start_cursor : str | int | None
        Initial cursor value for cursor-based pagination.
    records_path : str | None
        Dotted path to the records list inside each page payload.
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
    fetch : Callable[[str, Mapping[str, Any] | None, int | None], Any] | None
        Callback used to fetch a single page. It receives the absolute URL,
        the request params mapping, and the 1-based page index.
    sleep_seconds : float
        Number of seconds to sleep between page fetches. When non-positive,
        no sleeping occurs.
    sleep_func : Callable[[float], None] | None
        Function used to perform sleeping; typically wraps ``time.sleep``
        or a test double.
    last_page : int
        Tracks the last page index attempted. Useful for diagnostics.
    """

    # -- Constants -- #

    PAGE_PARAM: ClassVar[str] = 'page'
    SIZE_PARAM: ClassVar[str] = 'per_page'
    START_PAGE: ClassVar[int] = 1
    PAGE_SIZE: ClassVar[int] = 100
    CURSOR_PARAM: ClassVar[str] = 'cursor'
    LIMIT_PARAM: ClassVar[str] = 'limit'

    # -- Attributes -- #

    type: PaginationType = 'page'
    page_size: int = PAGE_SIZE
    start_page: int = START_PAGE
    start_cursor: str | int | None = None
    records_path: str | None = None
    cursor_path: str | None = None
    max_pages: int | None = None
    max_records: int | None = None
    page_param: str = PAGE_PARAM
    size_param: str = SIZE_PARAM
    cursor_param: str = CURSOR_PARAM
    limit_param: str = LIMIT_PARAM

    fetch: Callable[
        [str, Mapping[str, Any] | None, int | None], Any,
    ] | None = None
    sleep_seconds: float = 0.0
    sleep_func: Callable[[float], None] | None = None
    last_page: int = 0

    # -- Class Methods -- #

    @classmethod
    def from_config(
        cls,
        config: PaginationConfig,
        *,
        fetch: Callable[[str, Mapping[str, Any] | None, int | None], Any],
        sleep_func: Callable[[float], None] | None = None,
        sleep_seconds: float = 0.0,
    ) -> Paginator:
        """Normalize config and build a paginator instance."""
        ptype_raw = config.get('type') or 'page'
        ptype = ptype_raw.strip().lower()
        if ptype not in ('page', 'offset', 'cursor'):
            ptype = 'page'

        def _int(name: str, default: int) -> int:
            raw = config.get(name, default)
            try:
                val = int(cast(int | float | str, raw))
            except (TypeError, ValueError):
                return default
            return val

        page_size = _int('page_size', cls.PAGE_SIZE)
        if page_size < 1:
            page_size = 1

        start_page = _int('start_page', cls.START_PAGE)
        if ptype == 'page' and start_page < 1:
            start_page = 1
        if ptype == 'offset' and start_page < 0:
            start_page = 0

        return cls(
            type=ptype,  # type: ignore[arg-type]
            page_size=page_size,
            start_page=start_page,
            start_cursor=config.get('start_cursor'),
            records_path=config.get('records_path'),
            cursor_path=config.get('cursor_path'),
            max_pages=config.get('max_pages'),
            max_records=config.get('max_records'),
            page_param=config.get('page_param') or cls.PAGE_PARAM,
            size_param=config.get('size_param') or cls.SIZE_PARAM,
            cursor_param=config.get('cursor_param') or cls.CURSOR_PARAM,
            limit_param=config.get('limit_param') or cls.LIMIT_PARAM,
            fetch=fetch,
            sleep_seconds=sleep_seconds,
            sleep_func=sleep_func,
        )

    # Instance Methods -- #

    def paginate(
        self,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
    ) -> list[dict]:
        """Collect all records across pages into a list of dicts."""
        return list(self.paginate_iter(url, params=params))

    def paginate_iter(
        self,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
    ) -> Iterator[dict]:
        """Yield record dicts across pages for the configured strategy."""
        if self.fetch is None:
            raise ValueError('Paginator.fetch must be provided')

        ptype = self.type
        pages = 0
        recs = 0

        if ptype in ('page', 'offset'):
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
                        int(self.max_records) - (recs - n),
                    )
                    yield from batch[:take]
                    break

                yield from batch

                if n < self.page_size:
                    break
                if self._stop_limits(pages, recs):
                    break

                if ptype == 'page':
                    current += 1
                else:
                    current += self.page_size

                self._sleep()
            return

        if ptype == 'cursor':
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
                        int(self.max_records) - (recs - n),
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
                self._sleep()
            return

        # Fallback: single page, coalesce. and yield.
        self.last_page = 1
        page_data = self._fetch_page(url, params)
        yield from self.coalesce_records(page_data, self.records_path)

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

    # TODO: Replace with RateLimiter.
    def _sleep(self) -> None:
        """
        Sleep for the configured number of seconds.

        Uses the provided sleep function.
        """
        if self.sleep_func is None:
            return
        if self.sleep_seconds > 0:
            self.sleep_func(self.sleep_seconds)

    # -- Static Methods -- #

    @staticmethod
    def coalesce_records(
        x: Any,
        records_path: str | None,
    ) -> list[dict]:
        """Coalesce JSON page payloads into a list of dicts.

        Supports dotted path extraction via ``records_path`` and handles
        lists, maps, and scalars by coercing non-dict items into
        ``{"value": x}``.
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
                return Paginator.coalesce_records(items, None)
            return [data]

        return [{'value': data}]

    @staticmethod
    def next_cursor_from(
        data_obj: Any,
        path: str | None,
    ) -> str | int | None:
        """Extract a cursor value from a JSON payload using a dotted path."""
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
