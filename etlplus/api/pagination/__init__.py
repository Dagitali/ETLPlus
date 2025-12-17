"""
:mod:`etlplus.api.pagination` package.

Pagination configuration and runtime helpers for REST API responses.

This package groups configuration shapes, paginator utilities, and a
client-facing driver for traversing page-, offset-, and cursor-based JSON
responses.

Notes
-----
- Pagination defaults are centralized on :class:`EndpointClient` (``page``,
    ``per_page``, ``cursor``, ``limit``; start page ``1``; page size ``100``).
- Prefer :data:`JSONRecords` (list of :data:`JSONDict`) for paginated
    responses; scalar/record aliases are exported for convenience.
- The underlying :class:`Paginator` is exported for advanced scenarios that
    need to stream pages manually.
"""

from __future__ import annotations

from .client import PaginationClient
from .config import CursorPaginationConfigMap
from .config import PagePaginationConfigMap
from .config import PaginationConfig
from .config import PaginationConfigMap
from .config import PaginationInput
from .config import PaginationType
from .paginator import Paginator

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'PaginationClient',
    'Paginator',
    # Data Classes
    'PaginationConfig',
    # Enums
    'PaginationType',
    # Type Aliases
    'CursorPaginationConfigMap',
    'PagePaginationConfigMap',
    'PaginationInput',
    'PaginationConfigMap',
]
