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

from .rate_limiter import RateLimitConfig
from .rate_limiter import RateLimitConfigMap
from .rate_limiter import RateLimiter
from .rate_limiter import RateLimitPlan

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'RateLimiter',

    # Data Classes
    'RateLimitConfig',

    # Type Dicts
    'RateLimitConfigMap',
    'RateLimitPlan',

]
