"""
``tests.unit.api.test_u_rate`` module.

Unit tests for ``etlplus.api.rate``.

Notes
-----
- Ensures non-positive and non-numeric inputs result in 0.0 seconds.
"""
from __future__ import annotations

from typing import Any

import pytest

from etlplus.api.rate import compute_sleep_seconds
from etlplus.api.types import RateLimitConfig


# SECTION: TESTS ============================================================ #


def test_compute_sleep_seconds_overrides_sleep_seconds() -> None:
    """Test that sleep_seconds in config overrides other values."""
    assert compute_sleep_seconds(None, {'sleep_seconds': 0.2}) == 0.2


def test_compute_sleep_seconds_overrides_max_per_sec() -> None:
    """Test that max_per_sec in config overrides other values."""
    assert compute_sleep_seconds(None, {'max_per_sec': 4}) == 0.25


def test_compute_sleep_seconds_rate_limit_fallback() -> None:
    """Test fallback to rate limit config when override is None."""
    assert compute_sleep_seconds({'max_per_sec': 2}, None) == 0.5


@pytest.mark.parametrize(
    'rate_limit, config, expected',
    [
        (RateLimitConfig({'sleep_seconds': -1}), None, 0.0),
        (None, {'max_per_sec': 'oops'}, 0.0),
    ],
)
def test_compute_sleep_seconds_invalid_values(
    rate_limit: RateLimitConfig | None,
    config: RateLimitConfig | dict[str, Any] | None,
    expected: float,
) -> None:
    """
    Test that non-positive and non-numeric values are ignored and return 0.0.

    Parameters
    ----------
    rate_limit : RateLimitConfig | None
        The rate limit configuration.
    config : RateLimitConfig | dict[str, Any] | None
        The override configuration.
    expected : float
        The expected sleep seconds value.
    """
    assert compute_sleep_seconds(rate_limit, config) == expected
