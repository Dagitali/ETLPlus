"""
etlplus.config.pagination module.

Pagination model for REST API responses (page, offset, cursor styles).

Notes
-----
- TypedDict shapes are editor hints; runtime parsing remains permissive
    (``from_obj`` accepts ``Mapping[str, Any]``).
- Numeric fields are normalized with tolerant casts; ``validate_bounds``
    returns warnings instead of raising.

See Also
--------
- :meth:`PaginationConfig.validate_bounds`
- :func:`etlplus.config.utils.to_int`
- :func:`etlplus.config.utils.to_float`
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from typing import overload
from typing import Self
from typing import TYPE_CHECKING

from .mixins import BoundsWarningsMixin
from .utils import to_int

if TYPE_CHECKING:
    from ..api import PaginationConfigMap, PaginationType


# SECTION: EXPORTS ========================================================== #


__all__ = ['PaginationConfig']


# SECTION: CLASSES ========================================================== #


@dataclass(slots=True)
class PaginationConfig(BoundsWarningsMixin):
    """
    Configuration for pagination in API requests.

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

        # Page/offset
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
        Parse a mapping into a ``PaginationConfig`` instance.

        Parameters
        ----------
        obj : Mapping[str, Any] | None
            Mapping with optional pagination fields, or ``None``.

        Returns
        -------
        Self | None
            Parsed instance, or ``None`` if ``obj`` isn't a mapping.

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
            max_pages=to_int(obj.get('max_pages')),
            max_records=to_int(obj.get('max_records')),
        )


def _normalize_pagination_type(
    value: Any,
) -> PaginationType | None:
    """
    Normalize a value into a PaginationType enum member.

    Parameters
    ----------
    value : Any
        The value to normalize into a PaginationType.

    Returns
    -------
    PaginationType | None
        The normalized PaginationType, or None if unrecognized.
    """
    from ..api import PaginationType

    match str(value).strip().lower() if value is not None else '':
        case 'page':
            return PaginationType.PAGE
        case 'offset':
            return PaginationType.OFFSET
        case 'cursor':
            return PaginationType.CURSOR
        case _:
            return None
