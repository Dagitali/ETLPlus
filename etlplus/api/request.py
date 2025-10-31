"""
ETLPlus API Request
======================

REST API helpers for endpoint requests.
"""
from __future__ import annotations

from typing import Any


# SECTION: PROTECTED FUNCTIONS ============================================== #


def compute_sleep_seconds(
    rate_limit: Any | None,
    overrides: dict[str, Any] | None = None,
) -> float:
    """
    Compute sleep seconds from rate_limit config and optional overrides.

    Supports either explicit sleep_seconds or max_per_sec -> 1 / max_per_sec.
    """
    sleep_s: float = 0.0
    if rate_limit and getattr(rate_limit, 'sleep_seconds', None):
        try:
            # type: ignore[attr-defined]
            sleep_s = float(rate_limit.sleep_seconds)
        except (TypeError, ValueError):
            sleep_s = 0.0
    if rate_limit and getattr(rate_limit, 'max_per_sec', None):
        try:
            mps = float(rate_limit.max_per_sec)  # type: ignore[attr-defined]
            if mps > 0:
                sleep_s = 1.0 / mps
        except (TypeError, ValueError):
            pass
    if overrides:
        if 'sleep_seconds' in overrides:
            try:
                sleep_s = float(overrides['sleep_seconds'])
            except (TypeError, ValueError):
                pass
        if 'max_per_sec' in overrides:
            try:
                mps = float(overrides['max_per_sec'])
                if mps > 0:
                    sleep_s = 1.0 / mps
            except (TypeError, ValueError):
                pass
    return sleep_s
