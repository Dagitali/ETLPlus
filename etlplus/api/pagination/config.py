"""
:mod:`etlplus.api.pagination.config` module.

Pagination configuration shapes for REST API pagination.

This module defines the configuration schema for pagination strategies used
by :mod:`etlplus.api.pagination`. It exposes:

- :class:`PaginationType` – enumeration of supported pagination modes.
- :class:`PaginationConfig` – normalized configuration container.
- ``*PaginationConfigMap`` TypedDicts – loose, user-facing config mappings.

Notes
-----
- TypedDict shapes are editor hints; runtime parsing remains permissive
    (``from_obj`` accepts any :class:`collections.abc.Mapping`).
- Numeric fields are normalized with tolerant casts; ``validate_bounds``
    returns warnings instead of raising.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from typing import Literal
from typing import Required
from typing import Self
from typing import TypedDict
from typing import overload

from ...enums import CoercibleStrEnum
from ...mixins import BoundsWarningsMixin
from ...types import StrAnyMap
from ...utils import maybe_mapping
from ...utils import to_int

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Data Classes
    'PaginationConfig',
    # Enums
    'PaginationType',
    # Type Aliases
    'PaginationConfigMap',
    'PaginationInput',
    # Typed Dicts
    'CursorPaginationConfigMap',
    'PagePaginationConfigMap',
]


# SECTION: CONSTANTS ======================================================== #


_MISSING = object()


# SECTION: ENUMS ============================================================ #


class PaginationType(CoercibleStrEnum):
    """Enumeration of supported pagination types for REST API responses."""

    # -- Constants -- #

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


# SECTION: DATA CLASSES ===================================================== #


@dataclass(kw_only=True, slots=True)
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
    limit_param : str | None
        Query parameter name carrying the page size for cursor-based
        pagination when the API uses a separate limit field.
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
    limit_param: str | None = None

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
        limit_param = obj.get('limit_param')

        # Map from nested shapes when provided.
        if params_blk := maybe_mapping(obj.get('params')):
            page_param = page_param or params_blk.get('page')
            size_param = (
                size_param
                or params_blk.get('per_page')
                or params_blk.get('limit')
            )
            cursor_param = cursor_param or params_blk.get('cursor')
            fallback_path = fallback_path or params_blk.get('fallback_path')
        if resp_blk := maybe_mapping(obj.get('response')):
            records_path = records_path or resp_blk.get('items_path')
            cursor_path = cursor_path or resp_blk.get('next_cursor_path')
            fallback_path = fallback_path or resp_blk.get('fallback_path')
        if dflt_blk := maybe_mapping(obj.get('defaults')):
            page_size = page_size or dflt_blk.get('per_page')

        return cls(
            type=PaginationType.try_coerce(obj.get('type')),
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
            limit_param=limit_param,
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
            Parsed pagination configuration, or ``None`` if *obj* isn't a
            mapping.

        Notes
        -----
        Tolerant: unknown keys ignored; numeric fields coerced via
        ``to_int``; non-mapping inputs return ``None``.
        """
        if not isinstance(obj, Mapping):
            return None

        return cls(
            type=PaginationType.try_coerce(obj.get('type')),
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
            limit_param=obj.get('limit_param'),
        )


# SECTION: TYPE ALIASES ===================================================== #


type PaginationConfigMap = PagePaginationConfigMap | CursorPaginationConfigMap

# External callers may pass either a raw mapping-shaped config or an already
# constructed PaginationConfig instance, or omit pagination entirely. Accept a
# loose mapping here to reflect the runtime behavior while still providing
# stronger TypedDict hints for common shapes.
type PaginationInput = (
    PaginationConfigMap | PaginationConfig | StrAnyMap | None
)
