"""
etlplus.config.rate_limit
=========================

A module defining configuration types for rate limiting in REST API endpoint
requests.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


# SECTION: CLASSES ========================================================== #


@dataclass(slots=True)
class RateLimitConfig:
    """
    Configuration for rate limiting in API requests.

    Attributes
    ----------
    sleep_seconds : float | None
        Number of seconds to sleep between requests.
    max_per_sec : float | None
        Maximum number of requests per second.

    Methods
    -------
    from_obj(obj: Any) -> RateLimitConfig | None
        Create a RateLimitConfig instance from a dictionary-like object.
    """

    # -- Attributes -- #

    sleep_seconds: float | None = None
    max_per_sec: float | None = None

    # -- Static Methods -- #

    @staticmethod
    def from_obj(obj: Any) -> RateLimitConfig | None:
        if not isinstance(obj, dict):
            return None
        return RateLimitConfig(
            sleep_seconds=obj.get('sleep_seconds'),
            max_per_sec=obj.get('max_per_sec'),
        )
