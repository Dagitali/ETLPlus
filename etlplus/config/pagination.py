"""
etlplus.config.pagination
=========================

A module defining configuration types for REST API endpoint response
pagination.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from typing import Self


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

    # -- Class Methods -- #

    @classmethod
    def from_obj(cls, obj: Mapping[str, Any] | None) -> Self | None:
        if not isinstance(obj, Mapping):
            return None
        return cls(
            type=str(obj.get('type')) if obj.get('type') is not None else None,
            page_param=obj.get('page_param'),
            size_param=obj.get('size_param'),
            start_page=obj.get('start_page'),
            page_size=obj.get('page_size'),
            cursor_param=obj.get('cursor_param'),
            cursor_path=obj.get('cursor_path'),
            start_cursor=obj.get('start_cursor'),
            records_path=obj.get('records_path'),
            max_pages=obj.get('max_pages'),
            max_records=obj.get('max_records'),
        )
