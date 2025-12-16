"""
:mod:`etlplus.api.pagination` package.

High-level helpers for building REST API clients with pagination.

Notes
-----
- Pagination defaults are centralized on the client (``page``, ``per_page``,
    ``cursor``, ``limit``; start page ``1``; page size ``100``).
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
    'PaginationConfigMap',
]
