"""
:mod:`etlplus.api.rate_limiting` package.

High-level helpers for limiting HTTP request rates.

This subpackage exposes small, focused primitives for configuring and
enforcing HTTP request rate limits:

- :class:`RateLimitConfig` – immutable configuration for sleep and
    maximum requests-per-second.
- :class:`RateLimiter` – runtime helper that sleeps between requests
    according to a resolved configuration.

These utilities are intentionally minimal and orthogonal to the rest of
the API surface, following KISS and high cohesion/low coupling
principles.
"""
from __future__ import annotations

from .config import RateLimitConfig
from .config import RateLimitConfigMap
from .config import RateLimitOverrides
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
]
