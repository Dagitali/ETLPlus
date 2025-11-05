"""
etlplus.config.rate_limit
=========================

A module defining configuration types for rate limiting in REST API endpoint
requests.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from typing import overload
from typing import Self


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

    # -- Class Methods -- #

    @classmethod
    @overload
    def from_obj(
        cls,
        obj: Mapping[str, Any],
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
        Create a RateLimitConfig instance from a dictionary-like object.

        Parameters
        ----------
        obj : Mapping[str, Any] | None
            The object to parse (expected to be a mapping).

        Returns
        -------
        Self | None
            A RateLimitConfig instance, or None if parsing failed.
        """

        if not isinstance(obj, Mapping):
            return None
        kwargs = {k: obj.get(k) for k in ('sleep_seconds', 'max_per_sec')}

        return cls(**kwargs)
