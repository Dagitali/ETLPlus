"""
:mod:`tests.unit.api.test_u_api_rate_limiter` module.

Unit tests for :class:`etlplus.api.rate_limiting.RateLimiter`.

Notes
-----
- Ensures non-positive and non-numeric inputs result in 0.0 seconds.
- Ensures non-positive and non-numeric inputs result in a disabled limiter.
- Verifies helper constructors and configuration-based construction.

Examples
--------
>>> pytest tests/unit/api/test_u_api_rate_limiter.py
"""

from __future__ import annotations

from typing import Any

import pytest

import etlplus.api.rate_limiting as rate_limiting_pkg
from etlplus.api.rate_limiting import RateLimitConfigDict
from etlplus.api.rate_limiting import RateLimiter
from etlplus.api.rate_limiting import RateLimitInput

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestRateLimitingPackageExports:
    """Unit tests for rate-limiting package exports."""

    def test_rate_limit_input_is_reexported(self) -> None:
        """Test that the package facade exports the rate-limit input alias."""
        assert 'RateLimitInput' in rate_limiting_pkg.__all__
        assert rate_limiting_pkg.RateLimitInput is RateLimitInput


class TestResolveSleepSeconds:
    """
    Unit tests for :meth:`RateLimiter.resolve_sleep_seconds`.

    Notes
    -----
    - Ensures correct precedence and fallback logic for sleep_seconds and
        max_per_sec.
    - Validates handling of invalid, non-numeric, and non-positive values.

    Examples
    --------
    >>> RateLimiter.resolve_sleep_seconds({"max_per_sec": 2}, None)
    0.5
    """

    @pytest.mark.parametrize(
        ('rate_limit', 'overrides', 'expected_sleep'),
        [
            pytest.param(
                {'sleep_seconds': -1},
                None,
                0.0,
                id='negative-sleep-seconds',
            ),
            pytest.param(
                None,
                {'max_per_sec': 'oops'},  # type: ignore[typeddict-item]
                0.0,
                id='non-numeric-max-per-sec',
            ),
            pytest.param(None, {'max_per_sec': 4}, 0.25, id='override-max-per-sec'),
            pytest.param(None, {'sleep_seconds': 0.2}, 0.2, id='override-sleep'),
            pytest.param({'max_per_sec': 2}, None, 0.5, id='rate-limit-fallback'),
            pytest.param(
                {'sleep_seconds': 0.2, 'max_per_sec': 1},
                None,
                0.2,
                id='sleep-precedence',
            ),
        ],
    )
    def test_resolve_sleep_seconds_precedence_and_fallbacks(
        self,
        rate_limit: RateLimitConfigDict | None,
        overrides: RateLimitConfigDict | None,
        expected_sleep: float,
    ) -> None:
        """
        Test rate-limit overrides, fallbacks, invalid values, and precedence.
        """
        assert (
            RateLimiter.resolve_sleep_seconds(
                rate_limit=rate_limit,
                overrides=overrides,
            )
            == pytest.approx(expected_sleep)
        )


class TestRateLimiterBasics:
    """
    Unit tests for basic behavior of :class:`RateLimiter`.

    Notes
    -----
    - Tests disabled and fixed constructors.
    - Validates normalization and truthiness logic.

    Examples
    --------
    >>> RateLimiter.fixed(0.25)
    RateLimiter(...)
    """

    @pytest.mark.parametrize(
        ('limiter', 'expected_sleep', 'expected_enabled', 'expected_max_per_sec'),
        [
            pytest.param(RateLimiter.disabled(), 0.0, False, None, id='disabled'),
            pytest.param(RateLimiter.fixed(0.25), 0.25, True, 4.0, id='fixed'),
            pytest.param(
                RateLimiter(sleep_seconds=0.0, max_per_sec=4.0),
                0.25,
                True,
                4.0,
                id='derive-sleep-from-rate',
            ),
        ],
    )
    def test_constructor_state(
        self,
        limiter: RateLimiter,
        expected_sleep: float,
        expected_enabled: bool,
        expected_max_per_sec: float | None,
    ) -> None:
        """Test constructor state, truthiness, and derived sleep values."""
        assert limiter.sleep_seconds == pytest.approx(expected_sleep)
        assert limiter.enabled is expected_enabled
        assert bool(limiter) is expected_enabled
        assert limiter.max_per_sec == (
            None
            if expected_max_per_sec is None
            else pytest.approx(expected_max_per_sec)
        )

    @pytest.mark.parametrize(
        ('seconds', 'expected_sleep'),
        [
            (2.0, 2.0),
            (-1.0, 0.0),
            ('oops', 0.0),  # type: ignore[arg-type]
        ],
    )
    def test_fixed_normalizes_input(
        self,
        seconds: Any,
        expected_sleep: float,
    ) -> None:
        """
        Test that :meth:`RateLimiter.fixed` converts inputs to non-negative
        floats with a default of 0.0.
        """
        limiter = RateLimiter.fixed(seconds)  # type: ignore[arg-type]
        assert limiter.sleep_seconds == pytest.approx(expected_sleep)

    @pytest.mark.parametrize(
        ('seconds', 'expected_enabled'),
        [
            (0.0, False),
            (0.1, True),
            (-1.0, False),
        ],
    )
    def test_truthiness_and_enabled_flag(
        self,
        seconds: float,
        expected_enabled: bool,
    ) -> None:
        """
        Test that :class:`RateLimiter` truthiness and ``enabled`` follow
        ``sleep_seconds > 0``.
        """
        limiter = RateLimiter(sleep_seconds=seconds)
        assert limiter.enabled is expected_enabled
        assert bool(limiter) is expected_enabled


class TestRateLimiterFromConfig:
    """
    Unit tests for :meth:`RateLimiter.from_config` construction.

    Notes
    -----
    - Prefers sleep_seconds over max_per_sec.
    - Normalizes invalid values to 0.0.

    Examples
    --------
    >>> RateLimiter.from_config({'max_per_sec': 4})
    RateLimiter(...)
    """

    @pytest.mark.parametrize(
        ('cfg', 'expected_sleep'),
        [
            ({'sleep_seconds': 0.2}, 0.2),
            ({'sleep_seconds': '0.3'}, 0.3),
            ({'max_per_sec': 4}, 0.25),
            ({'max_per_sec': '5'}, 0.2),
            ({'sleep_seconds': -1, 'max_per_sec': -2}, 0.0),
            (None, 0.0),
        ],
    )
    def test_from_config(
        self,
        cfg: dict[str, Any] | None,
        expected_sleep: float,
    ) -> None:
        """
        Test that :meth:`from_config` prefers ``sleep_seconds`` over
        ``max_per_sec`` and normalizes invalid values to 0.0.
        """
        limiter = RateLimiter.from_config(cfg)
        assert limiter.sleep_seconds == pytest.approx(expected_sleep)


class TestRateLimiterEnforce:
    """
    Unit tests for :meth:`RateLimiter.enforce` behavior, covering enabled
    and disabled states.
    """

    @pytest.mark.parametrize(
        ('limiter', 'expected_calls'),
        [
            pytest.param(RateLimiter.fixed(0.5), [pytest.approx(0.5)], id='enabled'),
            pytest.param(RateLimiter.disabled(), [], id='disabled'),
        ],
    )
    def test_enforce_sleep_behavior(
        self,
        monkeypatch: pytest.MonkeyPatch,
        limiter: RateLimiter,
        expected_calls: list[float],
    ) -> None:
        """Test that ``enforce`` sleeps only when the limiter is enabled."""
        calls: list[float] = []
        monkeypatch.setattr(
            'etlplus.api.rate_limiting._rate_limiter.time.sleep',
            calls.append,
        )

        limiter.enforce()

        assert calls == expected_calls
