"""
etlplus.config.pagination
=========================

A module defining configuration types for REST API endpoint response
pagination.

Notes
-----
TypedDict shapes are editor hints; runtime parsing remains permissive (from_obj
accepts Mapping[str, Any]).
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from typing import overload
from typing import Self
from typing import TYPE_CHECKING

from .utils import to_int

if TYPE_CHECKING:
    from .types import PaginationConfigMap


# SECTION: EXPORTS ========================================================== #


__all__ = ['PaginationConfig']


# SECTION: CLASSES ========================================================== #


@dataclass(slots=True)
class PaginationConfig:
    """
    Configuration for pagination in API requests.

    Attributes
    ----------
    type : str | None
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
    max_pages : int | None
        Maximum number of pages to retrieve.
    max_records : int | None
        Maximum number of records to retrieve.

    Methods
    -------
    from_obj(obj: Any) -> PaginationConfig | None
        Create a PaginationConfig instance from a dictionary-like object.
    """

    # -- Attributes -- #

    type: str | None = None  # "page" | "offset" | "cursor"

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
            A list of warning messages (empty if all values look sane).
        """

        warnings: list[str] = []

        # General limits
        if (mp := self.max_pages) is not None and mp <= 0:
            warnings.append('max_pages should be > 0')
        if (mr := self.max_records) is not None and mr <= 0:
            warnings.append('max_records should be > 0')

        # Page/offset
        match (self.type or '').strip().lower():
            case 'page' | 'offset':
                if (sp := self.start_page) is not None and sp < 1:
                    warnings.append('start_page should be >= 1')
                if (ps := self.page_size) is not None and ps <= 0:
                    warnings.append('page_size should be > 0')
            case 'cursor':
                if (ps := self.page_size) is not None and ps <= 0:
                    warnings.append(
                        'page_size should be > 0 for cursor pagination',
                    )
            case _:
                pass

        return warnings

    # -- Class Methods -- #

    @classmethod
    @overload
    def from_obj(
        cls,
        obj: PaginationConfigMap,
    ) -> Self: ...

    @classmethod
    @overload
    def from_obj(
        cls,
        obj: None,
    ) -> None: ...

    @classmethod
    def from_obj(
        cls,
        obj: Mapping[str, Any] | None,
    ) -> Self | None:
        """
        Create a PaginationConfig instance from a dictionary-like object.

        Parameters
        ----------
        obj : Mapping[str, Any] | None
            The object to parse (expected to be a mapping).
        """

        if not isinstance(obj, Mapping):
            return None

        # Normalize type to str when present; cast numeric fields.
        t = obj.get('type')

        return cls(
            type=str(t) if t is not None else None,
            page_param=obj.get('page_param'),
            size_param=obj.get('size_param'),
            start_page=to_int(obj.get('start_page')),
            page_size=to_int(obj.get('page_size')),
            cursor_param=obj.get('cursor_param'),
            cursor_path=obj.get('cursor_path'),
            start_cursor=obj.get('start_cursor'),
            records_path=obj.get('records_path'),
            max_pages=to_int(obj.get('max_pages')),
            max_records=to_int(obj.get('max_records')),
        )
