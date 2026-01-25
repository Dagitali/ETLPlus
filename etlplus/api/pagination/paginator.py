"""
:mod:`etlplus.api.pagination.paginator` module.

Core pagination runtime for REST API responses.

This module implements :class:`Paginator`, which encapsulates pagination
behavior for page-, offset-, and cursor-based APIs. It delegates configuration
parsing to :mod:`etlplus.api.pagination.config` and focuses on executing
requests, extracting records, and enforcing limits.

Examples
--------
>>> from etlplus.api.pagination import Paginator, PaginationType
>>> from etlplus.api.types import RequestOptions, Url
>>> def fetch(url: Url, req: RequestOptions, page: int | None) -> dict:
...     ...  # issue HTTP request and return JSON payload
>>> paginator = Paginator(type=PaginationType.PAGE, page_size=100)
>>> rows = list(paginator.paginate_iter('https://api.example.com/items'))
"""

from __future__ import annotations

from collections.abc import Generator
from collections.abc import Mapping
from dataclasses import dataclass
from functools import partial
from typing import Any
from typing import ClassVar
from typing import cast

from ...types import JSONDict
from ...types import JSONRecords
from ...utils import to_int
from ...utils import to_maximum_int
from ...utils import to_positive_int
from ..errors import ApiRequestError
from ..errors import PaginationError
from ..rate_limiting import RateLimiter
from ..types import FetchPageCallable
from ..types import RequestOptions
from ..types import Url
from .config import PaginationConfig
from .config import PaginationInput
from .config import PaginationType

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'Paginator',
]


# SECTION: CONSTANTS ======================================================== #


_MISSING = object()


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _resolve_path(
    obj: Any,
    path: str | None,
) -> Any:
    """
    Resolve dotted *path* within *obj* or return ``_MISSING``.

    Parameters
    ----------
    obj : Any
        JSON payload from an API response.
    path : str | None
        Dotted path to the target value within *obj*.

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
        config: PaginationInput,
        *,
        fetch: FetchPageCallable,
        rate_limiter: RateLimiter | None = None,
    ) -> Paginator:
        """
        Normalize config and build a paginator instance.

        Parameters
        ----------
        config : PaginationInput
            Pagination configuration mapping or :class:`PaginationConfig`.
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
        # Normalize configuration into a mapping for downstream helpers.
        if isinstance(config, PaginationConfig):
            cfg: Mapping[str, Any] = {
                'type': config.type,
                'page_param': config.page_param,
                'size_param': config.size_param,
                'start_page': config.start_page,
                'page_size': config.page_size,
                'cursor_param': config.cursor_param,
                'cursor_path': config.cursor_path,
                'start_cursor': config.start_cursor,
                'records_path': config.records_path,
                'fallback_path': config.fallback_path,
                'max_pages': config.max_pages,
                'max_records': config.max_records,
                'limit_param': config.limit_param,
            }
        else:
            cfg = cast(Mapping[str, Any], config or {})

        ptype = cls.detect_type(cfg, default=PaginationType.PAGE)
        assert ptype is not None

        return cls(
            type=ptype,
            page_size=to_positive_int(cfg.get('page_size'), cls.PAGE_SIZE),
            start_page=to_maximum_int(
                cfg.get('start_page'),
                cls.START_PAGES[ptype],
            ),
            start_cursor=cfg.get('start_cursor'),
            records_path=cfg.get('records_path'),
            fallback_path=cfg.get('fallback_path'),
            cursor_path=cfg.get('cursor_path'),
            max_pages=to_int(cfg.get('max_pages'), None, minimum=1),
            max_records=to_int(cfg.get('max_records'), None, minimum=1),
            page_param=cfg.get('page_param', ''),
            size_param=cfg.get('size_param', ''),
            cursor_param=cfg.get('cursor_param', ''),
            limit_param=cfg.get('limit_param', ''),
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
            Request metadata snapshot reused across pages.

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
                {self.cursor_param: cursor} if cursor is not None else None
            )
            combined: dict[str, Any] = {
                self.limit_param: self.page_size,
            } | dict(request.params or {})
            if overrides:
                combined |= {
                    k: v for k, v in overrides.items() if v is not None
                }
            req_options = request.evolve(params=combined)

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
            req_options = request.evolve(params=merged)
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
        self,
        pages: int,
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
            Secondary dotted path consulted when *records_path* resolves to
            ``None`` or an empty list.

        Returns
        -------
        JSONRecords
            List of record dicts extracted from the payload.

        Notes
        -----
        Supports dotted path extraction via *records_path* and handles lists,
        mappings, and scalars by coercing non-dict items into ``{"value": x}``.
        """
        resolver = partial(_resolve_path, x)
        data = resolver(records_path)
        if data is _MISSING:
            data = None

        if fallback_path and (
            data is None or (isinstance(data, list) and not data)
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
            Detected pagination type, or *default* if not found.
        """
        if not config:
            return default

        raw = config.get('type')
        if raw is None:
            return default

        # Delegate normalization to CoercibleStrEnum implementation,
        # allowing aliases and consistent error handling.
        coerced = PaginationType.try_coerce(raw)
        return coerced if coerced is not None else default

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
        if not (isinstance(path, str) and path and isinstance(data_obj, dict)):
            return None
        cur: Any = data_obj
        for part in path.split('.'):
            if isinstance(cur, dict):
                cur = cur.get(part)
            else:
                return None
        return cur if isinstance(cur, (str, int)) else None
