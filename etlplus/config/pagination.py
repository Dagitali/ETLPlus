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

        # Normalize type to str when present; pass through other keys directly.
        t = obj.get('type')
        kwargs = {
            k: obj.get(k) for k in (
                'page_param', 'size_param', 'start_page', 'page_size',
                'cursor_param', 'cursor_path', 'start_cursor',
                'records_path', 'max_pages', 'max_records',
            )
        }
        return cls(type=str(t) if t is not None else None, **kwargs)
