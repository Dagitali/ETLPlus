"""
etlplus.config.rate_limit
=========================

A module defining configuration types for rate limiting in REST API endpoint
requests.

Notes
-----
TypedDict shapes are editor hints; runtime parsing remains permissive (from_obj
accepts Mapping[str, Any]).
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from typing import overload
from typing import Self
from typing import TYPE_CHECKING

from .mixins import BoundsWarningsMixin
from .utils import to_float

if TYPE_CHECKING:
    from .types import RateLimitConfigMap


# SECTION: EXPORTS ========================================================== #


__all__ = ['RateLimitConfig']


# SECTION: CLASSES ========================================================== #


@dataclass(slots=True)
class RateLimitConfig(BoundsWarningsMixin):
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

    # -- Instance Methods -- #

    def validate_bounds(self) -> list[str]:
        """
        Return non-raising warnings for suspicious numeric bounds.

        Returns
        -------
        list[str]
            A list of warning messages (empty if values look sane).
        """

        warnings: list[str] = []
        self._warn_if(
            (ss := self.sleep_seconds) is not None and ss < 0,
            'sleep_seconds should be >= 0',
            warnings,
        )
        self._warn_if(
            (mps := self.max_per_sec) is not None and mps <= 0,
            'max_per_sec should be > 0',
            warnings,
        )

        return warnings

    # -- Class Methods -- #

    @classmethod
    @overload
    def from_obj(
        cls,
        obj: RateLimitConfigMap,
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

        return cls(
            sleep_seconds=to_float(obj.get('sleep_seconds')),
            max_per_sec=to_float(obj.get('max_per_sec')),
        )
