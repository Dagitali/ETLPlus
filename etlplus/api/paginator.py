"""
:mod:`etlplus.api.paginator` module.

Centralized logic for handling REST API endpoint responses, including:
- Pagination strategies (page, offset, cursor).
- Record extraction from JSON payloads.

This module provides a :class:`Paginator` class that encapsulates pagination
configuration and behavior. It supports instantiation from a configuration
mapping, fetching pages via a supplied callback, and iterating over records
across pages.

Notes
-----
- TypedDict shapes are editor hints; runtime parsing remains permissive
    (``from_obj`` accepts ``Mapping[str, Any]``).
- Numeric fields are normalized with tolerant casts; ``validate_bounds``
    returns warnings instead of raising.

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
>>> def fetch(url, request, page):
...     response = requests.get(url, params=request.params)
...     response.raise_for_status()
...     return response.json()
>>> paginator = Paginator.from_config(
...     cfg,
...     fetch=fetch,
...     rate_limiter=RateLimiter.fixed(0.5),
... )
>>> all_records = paginator.paginate('https://api.example.com/v1/items')

See Also
--------
- :meth:`PaginationConfig.validate_bounds`
- :func:`etlplus.config.utils.to_int`
- :func:`etlplus.config.utils.to_float`
"""
from __future__ import annotations

from collections.abc import Generator
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from functools import partial
from typing import Any
from typing import ClassVar
from typing import Literal
from typing import Required
from typing import Self
from typing import TypedDict
from typing import cast
from typing import overload

from ..mixins import BoundsWarningsMixin
from ..types import JSONDict
from ..types import JSONRecords
from ..types import StrAnyMap
from ..utils import to_int
from ..utils import to_maximum_int
from ..utils import to_positive_int
from ._parsing import maybe_mapping
from .errors import ApiRequestError
from .errors import PaginationError
from .rate_limiter import RateLimiter
from .types import FetchPageCallable
from .types import RequestOptions
from .types import Url

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'Paginator',

    # Data Classes
    'PaginationConfig',

    # Enums
    'PaginationType',

    # Typed Dicts
    'CursorPaginationConfigMap',
    'PagePaginationConfigMap',
    'PaginationConfigMap',
]


# SECTION: CONSTANTS ======================================================== #


_MISSING = object()


# SECTION: INTERNAL HELPERS ================================================ #


def _normalize_pagination_type(
    value: Any,
) -> PaginationType | None:
    """
    Normalize a value into a :class:`PaginationType` enum member.

    Parameters
    ----------
    value : Any
        Input value to normalize.

    Returns
    -------
    PaginationType | None
        Corresponding enum member, or ``None`` if unrecognized.
    """
    match str(value).strip().lower() if value is not None else '':
        case 'page':
            return PaginationType.PAGE
        case 'offset':
            return PaginationType.OFFSET
        case 'cursor':
            return PaginationType.CURSOR
        case _:
            return None


def _resolve_path(
    obj: Any,
    path: str | None,
) -> Any:
    """
    Resolve dotted ``path`` within ``obj`` or return ``_MISSING``.

    Parameters
    ----------
    obj : Any
        JSON payload from an API response.
    path : str | None
        Dotted path to the target value within ``obj``.

    Returns
    -------
    Any
        Target value from the payload, or ``_MISSING`` if the path does not
        exist.
    """
    if not isinstance(path, str) or not path:
        return obj
    cur: Any = obj
    for part in path.split('.'):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return _MISSING
    return cur


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


type PaginationConfigMap = PagePaginationConfigMap | CursorPaginationConfigMap


# SECTION: DATA CLASSES ===================================================== #


@dataclass(slots=True)
class PaginationConfig(BoundsWarningsMixin):
    """
    Configuration container for API request pagination settings.

    Attributes
    ----------
    type : PaginationType | None
        Pagination type: "page", "offset", or "cursor".
    page_param : str | None
        Name of the page parameter.
    size_param : str | None
        Name of the page size parameter.
    start_page : int | None
        Starting page number.
    page_size : int | None
        Number of records per page.
    cursor_param : str | None
        Name of the cursor parameter.
    cursor_path : str | None
        JSONPath expression to extract the cursor from the response.
    start_cursor : str | int | None
        Starting cursor value.
    records_path : str | None
        JSONPath expression to extract the records from the response.
    fallback_path : str | None
        Secondary JSONPath checked when ``records_path`` yields nothing.
    max_pages : int | None
        Maximum number of pages to retrieve.
    max_records : int | None
        Maximum number of records to retrieve.
    """

    # -- Attributes -- #

    type: PaginationType | None = None  # "page" | "offset" | "cursor"

    # Page/offset
    page_param: str | None = None
    size_param: str | None = None
    start_page: int | None = None
    page_size: int | None = None

    # Cursor
    cursor_param: str | None = None
    cursor_path: str | None = None
    start_cursor: str | int | None = None

    # General
    records_path: str | None = None
    fallback_path: str | None = None
    max_pages: int | None = None
    max_records: int | None = None

    # -- Instance Methods -- #

    def validate_bounds(self) -> list[str]:
        """
        Return non-raising warnings for suspicious numeric bounds.

        Uses structural pattern matching to keep branching concise.

        Returns
        -------
        list[str]
            Warning messages (empty if all values look sane).
        """
        warnings: list[str] = []

        # General limits
        self._warn_if(
            (mp := self.max_pages) is not None and mp <= 0,
            'max_pages should be > 0',
            warnings,
        )
        self._warn_if(
            (mr := self.max_records) is not None and mr <= 0,
            'max_records should be > 0',
            warnings,
        )

        match (self.type or '').strip().lower():
            case 'page' | 'offset':
                self._warn_if(
                    (sp := self.start_page) is not None and sp < 1,
                    'start_page should be >= 1',
                    warnings,
                )
                self._warn_if(
                    (ps := self.page_size) is not None and ps <= 0,
                    'page_size should be > 0',
                    warnings,
                )
            case 'cursor':
                self._warn_if(
                    (ps := self.page_size) is not None and ps <= 0,
                    'page_size should be > 0 for cursor pagination',
                    warnings,
                )
            case _:
                pass

        return warnings

    # -- Class Methods -- #

    @classmethod
    def from_defaults(
        cls,
        obj: StrAnyMap | None,
    ) -> Self | None:
        """
        Parse nested defaults mapping used by profile + endpoint configs.

        Parameters
        ----------
        obj : StrAnyMap | None
            Defaults mapping (non-mapping inputs return ``None``).

        Returns
        -------
        Self | None
            A :class:`PaginationConfig` instance with numeric fields coerced to
            int/float where applicable, or ``None`` if parsing failed.
        """
        if not isinstance(obj, Mapping):
            return None

        # Start with direct keys if present.
        page_param = obj.get('page_param')
        size_param = obj.get('size_param')
        start_page = obj.get('start_page')
        page_size = obj.get('page_size')
        cursor_param = obj.get('cursor_param')
        cursor_path = obj.get('cursor_path')
        start_cursor = obj.get('start_cursor')
        records_path = obj.get('records_path')
        fallback_path = obj.get('fallback_path')
        max_pages = obj.get('max_pages')
        max_records = obj.get('max_records')

        # Map from nested shapes when provided.
        if (params_blk := maybe_mapping(obj.get('params'))):
            page_param = page_param or params_blk.get('page')
            size_param = (
                size_param
                or params_blk.get('per_page')
                or params_blk.get('limit')
            )
            cursor_param = cursor_param or params_blk.get('cursor')
            fallback_path = fallback_path or params_blk.get('fallback_path')
        if (resp_blk := maybe_mapping(obj.get('response'))):
            records_path = records_path or resp_blk.get('items_path')
            cursor_path = cursor_path or resp_blk.get('next_cursor_path')
            fallback_path = fallback_path or resp_blk.get('fallback_path')
        if (dflt_blk := maybe_mapping(obj.get('defaults'))):
            page_size = page_size or dflt_blk.get('per_page')

        return cls(
            type=_normalize_pagination_type(obj.get('type')),
            page_param=page_param,
            size_param=size_param,
            start_page=to_int(start_page),
            page_size=to_int(page_size),
            cursor_param=cursor_param,
            cursor_path=cursor_path,
            start_cursor=start_cursor,
            records_path=records_path,
            fallback_path=fallback_path,
            max_pages=to_int(max_pages),
            max_records=to_int(max_records),
        )

    @classmethod
    @overload
    def from_obj(
        cls,
        obj: None,
    ) -> None: ...

    @classmethod
    @overload
    def from_obj(
        cls,
        obj: PaginationConfigMap,
    ) -> Self: ...

    @classmethod
    def from_obj(
        cls,
        obj: Mapping[str, Any] | None,
    ) -> Self | None:
        """
        Parse a mapping into a :class:`PaginationConfig` instance.

        Parameters
        ----------
        obj : Mapping[str, Any] | None
            Mapping with optional pagination fields, or ``None``.

        Returns
        -------
        Self | None
            Parsed pagination configuration, or ``None`` if ``obj`` isn't a
            mapping.

        Notes
        -----
        Tolerant: unknown keys ignored; numeric fields coerced via
        ``to_int``; non-mapping inputs return ``None``.
        """
        if not isinstance(obj, Mapping):
            return None

        return cls(
            type=_normalize_pagination_type(obj.get('type')),
            page_param=obj.get('page_param'),
            size_param=obj.get('size_param'),
            start_page=to_int(obj.get('start_page')),
            page_size=to_int(obj.get('page_size')),
            cursor_param=obj.get('cursor_param'),
            cursor_path=obj.get('cursor_path'),
            start_cursor=obj.get('start_cursor'),
            records_path=obj.get('records_path'),
            fallback_path=obj.get('fallback_path'),
            max_pages=to_int(obj.get('max_pages')),
            max_records=to_int(obj.get('max_records')),
        )


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
    fetch : FetchPageCallable | None
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

    def __post_init__(self) -> None:
        """
        Normalize and validate pagination configuration.
        """
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

    fetch: FetchPageCallable | None = None
    rate_limiter: RateLimiter | None = None
    last_page: int = 0

    # -- Class Methods -- #

    @classmethod
    def from_config(
        cls,
        config: Mapping[str, Any],
        *,
        fetch: FetchPageCallable,
        rate_limiter: RateLimiter | None = None,
    ) -> Paginator:
        """
        Normalize config and build a paginator instance.

        Parameters
        ----------
        config : Mapping[str, Any]
            Pagination configuration mapping.
        fetch : FetchPageCallable
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
        ptype = cls.detect_type(config, default=PaginationType.PAGE)
        assert ptype is not None

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
            cursor_path=config.get('cursor_path'),
            max_pages=to_int(config.get('max_pages'), None, minimum=1),
            max_records=to_int(config.get('max_records'), None, minimum=1),
            page_param=config.get('page_param', ''),
            size_param=config.get('size_param', ''),
            cursor_param=config.get('cursor_param', ''),
            limit_param=config.get('limit_param', ''),
            fetch=fetch,
            rate_limiter=rate_limiter,
        )

    # -- Instance Methods -- #

    def paginate(
        self,
        url: Url,
        *,
        request: RequestOptions | None = None,
    ) -> JSONRecords:
        """
        Collect all records across pages into a list of dicts.

        Parameters
        ----------
        url : Url
            Absolute URL of the endpoint to fetch.
        request : RequestOptions | None, optional
            Request metadata snapshot reused across pages. Provide
            ``RequestOptions.with_params`` to override query parameters.

        Returns
        -------
        JSONRecords
            List of record dicts aggregated across all fetched pages.
        """
        prepared = request or RequestOptions()
        return list(self.paginate_iter(url, request=prepared))

    def paginate_iter(
        self,
        url: Url,
        *,
        request: RequestOptions | None = None,
    ) -> Generator[JSONDict]:
        """
        Yield record dicts across pages for the configured strategy.

        Parameters
        ----------
        url : Url
            Absolute URL of the endpoint to fetch.
        request : RequestOptions | None, optional
            Pre-built request metadata snapshot to clone per page.

        Yields
        ------
        Generator[JSONDict]
            Iterator over the record dicts extracted from paginated responses.

        Raises
        ------
        ValueError
            If ``fetch`` callback is not provided.
        """
        if self.fetch is None:
            raise ValueError('Paginator.fetch must be provided')

        base_request = request or RequestOptions()

        match self.type:
            case PaginationType.PAGE | PaginationType.OFFSET:
                yield from self._iterate_page_style(url, base_request)
                return
            case PaginationType.CURSOR:
                yield from self._iterate_cursor_style(url, base_request)
                return

    # -- Internal Instance Methods -- #

    def _enforce_rate_limit(self) -> None:
        """Apply configured pacing between subsequent page fetches."""
        if self.rate_limiter is not None:
            self.rate_limiter.enforce()

    def _fetch_page(
        self,
        url: Url,
        request: RequestOptions,
    ) -> Any:
        """
        Fetch a single page and attach page index on failure.

        When the underlying ``fetch`` raises :class:`ApiRequestError`, this
        helper re-raises :class:`PaginationError` with the current
        ``last_page`` value populated so callers can inspect the failing
        page index.

        Parameters
        ----------
        url : Url
            Absolute URL of the endpoint to fetch.
        request : RequestOptions
            Request metadata (params/headers/timeout) for the fetch.

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
            return self.fetch(url, request, self.last_page)
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

    def _iterate_cursor_style(
        self,
        url: Url,
        request: RequestOptions,
    ) -> Generator[JSONDict]:
        """
        Yield record dicts for cursor-based pagination strategies.

        Parameters
        ----------
        url : Url
            Endpoint URL to paginate.
        request : RequestOptions
            Base request metadata passed by the caller.

        Yields
        ------
        Generator[JSONDict]
            Iterator over normalized record dictionaries for each page.
        """
        cursor = self.start_cursor
        pages = 0
        emitted = 0

        while True:
            self.last_page = pages + 1
            overrides = (
                {self.cursor_param: cursor}
                if cursor is not None
                else None
            )
            combined: dict[str, Any] = (
                {self.limit_param: self.page_size}
                | dict(request.params or {})
            )
            if overrides:
                combined |= {
                    k: v
                    for k, v in overrides.items()
                    if v is not None
                }
            req_options = request.with_params(combined)

            page_data = self._fetch_page(url, req_options)
            batch = self.coalesce_records(
                page_data,
                self.records_path,
                self.fallback_path,
            )

            pages += 1
            trimmed, exhausted = self._limit_batch(batch, emitted)
            yield from trimmed
            emitted += len(trimmed)

            nxt = self.next_cursor_from(page_data, self.cursor_path)
            if exhausted or not nxt or not batch:
                break
            if self._stop_limits(pages, emitted):
                break

            cursor = nxt
            self._enforce_rate_limit()

    def _iterate_page_style(
        self,
        url: Url,
        request: RequestOptions,
    ) -> Generator[JSONDict]:
        """
        Yield record dicts for page/offset pagination strategies.

        Parameters
        ----------
        url : Url
            Endpoint URL to paginate.
        request : RequestOptions
            Base request metadata passed by the caller.

        Yields
        ------
        Generator[JSONDict]
            Iterator over normalized record dictionaries for each page.
        """
        current = self._resolve_start_page(request)
        pages = 0
        emitted = 0

        while True:
            self.last_page = pages + 1
            merged = dict(request.params or {}) | {
                self.page_param: current,
                self.size_param: self.page_size,
            }
            req_options = request.with_params(merged)
            page_data = self._fetch_page(url, req_options)
            batch = self.coalesce_records(
                page_data,
                self.records_path,
                self.fallback_path,
            )

            pages += 1
            trimmed, exhausted = self._limit_batch(batch, emitted)
            yield from trimmed
            emitted += len(trimmed)

            if exhausted or len(batch) < self.page_size:
                break
            if self._stop_limits(pages, emitted):
                break

            current = self._next_page_value(current)
            self._enforce_rate_limit()

    def _limit_batch(
        self,
        batch: JSONRecords,
        emitted: int,
    ) -> tuple[JSONRecords, bool]:
        """Respect ``max_records`` while yielding the current batch.

        Parameters
        ----------
        batch : JSONRecords
            Records retrieved from the latest page fetch.
        emitted : int
            Count of records yielded so far.

        Returns
        -------
        tuple[JSONRecords, bool]
            ``(records_to_emit, exhausted)`` where ``exhausted`` indicates
            the ``max_records`` limit was reached.
        """
        if not isinstance(self.max_records, int):
            return batch, False

        remaining = self.max_records - emitted
        if remaining <= 0:
            return [], True
        if len(batch) > remaining:
            return batch[:remaining], True
        return batch, False

    def _next_page_value(
        self,
        current: int,
    ) -> int:
        """
        Return the next page/offset value for the active strategy.

        Parameters
        ----------
        current : int
            Current page number or offset value.

        Returns
        -------
        int
            Incremented page number or offset respecting pagination type.
        """
        if self.type == PaginationType.OFFSET:
            return current + self.page_size
        return current + 1

    def _resolve_start_page(
        self,
        request: RequestOptions,
    ) -> int:
        """
        Allow per-call overrides of the first page via request params.

        Parameters
        ----------
        request : RequestOptions
            Request metadata snapshot passed by the caller.

        Returns
        -------
        int
            Starting page number or offset for this pagination session.
        """
        if not request.params:
            return self.start_page
        maybe = request.params.get(self.page_param)
        if maybe is None:
            return self.start_page
        parsed = to_int(maybe)
        if parsed is None:
            return self.start_page
        if self.type == PaginationType.OFFSET:
            return parsed if parsed >= 0 else self.START_PAGES[self.type]
        return parsed if parsed >= 1 else self.START_PAGES[self.type]

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
        resolver = partial(_resolve_path, x)
        data = resolver(records_path)
        if data is _MISSING:
            data = None

        if fallback_path and (
            data is None
            or (isinstance(data, list) and not data)
        ):
            fallback = resolver(fallback_path)
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
    def detect_type(
        config: Mapping[str, Any] | None,
        *,
        default: PaginationType | None = None,
    ) -> PaginationType | None:
        """
        Return a normalized pagination type when possible.

        Parameters
        ----------
        config : Mapping[str, Any] | None
            Pagination configuration mapping.
        default : PaginationType | None, optional
            Default type to return when not specified in config.

        Returns
        -------
        PaginationType | None
            Detected pagination type, or ``default`` if not found.
        """
        if not config:
            return default
        raw = config.get('type')
        if isinstance(raw, PaginationType):
            return raw
        if raw is None:
            return default
        try:
            return PaginationType(str(raw).strip().lower())
        except ValueError:
            return default

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
