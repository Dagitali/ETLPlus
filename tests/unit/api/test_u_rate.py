"""
``tests.unit.api.test_u_rate`` module.

Unit tests for ``etlplus.api.rate``.

Notes
-----
- Ensures non-positive and non-numeric inputs result in 0.0 seconds.

Examples
--------
>>> pytest tests/unit/api/test_u_rate.py
"""
from __future__ import annotations

from typing import Any

import pytest

from etlplus.api.rate import compute_sleep_seconds
from etlplus.api.types import RateLimitConfig


# SECTION: TESTS ============================================================ #


@pytest.mark.unit
@pytest.mark.usefixtures()
class TestComputeSleepSeconds:
    """
    Unit test suite for :func:`compute_sleep_seconds`.

    Notes
    -----
    - Ensures correct precedence and fallback logic for sleep_seconds and
        max_per_sec.
    - Validates handling of invalid, non-numeric, and non-positive values.
    """

    @pytest.mark.parametrize(
        'rate_limit, config, expected',
        [
            (RateLimitConfig({'sleep_seconds': -1}), None, 0.0),
            (None, {'max_per_sec': 'oops'}, 0.0),
        ],
    )
    def test_invalid_values(
        self,
        rate_limit: RateLimitConfig | None,
        config: RateLimitConfig | dict[str, Any] | None,
        expected: float,
    ) -> None:
        """
        Test that non-positive and non-numeric values are ignored and return
        0.0.

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

    def test_overrides_max_per_sec(self) -> None:
        """Test that max_per_sec in config overrides other values."""
        assert compute_sleep_seconds(None, {'max_per_sec': 4}) == 0.25

    def test_overrides_sleep_seconds(self) -> None:
        """Test that sleep_seconds in config overrides other values."""
        assert compute_sleep_seconds(None, {'sleep_seconds': 0.2}) == 0.2

    def test_rate_limit_fallback(self) -> None:
        """Test fallback to rate limit config when override is None."""
        assert compute_sleep_seconds({'max_per_sec': 2}, None) == 0.5
