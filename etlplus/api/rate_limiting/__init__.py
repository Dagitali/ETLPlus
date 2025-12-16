"""
:mod:`etlplus.api.rate_limiting` package.

High-level helpers for building REST API clients with rate limiting.

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

from .config import RateLimitConfig
from .config import RateLimitConfigMap
from .config import RateLimitOverrides
from .config import RateLimitPlan
from .rate_limiter import RateLimiter

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'RateLimiter',

    # Data Classes
    'RateLimitConfig',

    # Type Aliases
    'RateLimitOverrides',

    # Type Dicts
    'RateLimitConfigMap',
    'RateLimitPlan',

]
