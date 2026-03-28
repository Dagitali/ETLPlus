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
from typing import cast

import pytest

from etlplus.api.rate_limiting import RateLimitConfigDict
from etlplus.api.rate_limiting import RateLimiter

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: FIXTURES ======================================================== #


@pytest.fixture(name='disabled_limiter')
def disabled_limiter_fixture() -> RateLimiter:
    """
    Fixture returning a :class:`RateLimiter` instance with no delay.
    """
    return RateLimiter.disabled()


@pytest.fixture(name='fixed_limiter')
def fixed_limiter_fixture() -> RateLimiter:
    """
    Fixture returning a :class:`RateLimiter` instance with a small delay.
    """
    return RateLimiter.fixed(0.25)


# SECTION: TESTS ============================================================ #


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
        ('rate_limit', 'config', 'expected_sleep'),
        [
            pytest.param(
                {'sleep_seconds': -1},
                None,
                0.0,
                id='negative_sleep_seconds',
            ),
            pytest.param(
                None,
                {'max_per_sec': 'oops'},
                0.0,
                id='non_numeric_max_per_sec',
            ),
        ],
    )
    def test_invalid_values(
        self,
        rate_limit: RateLimitConfigDict | None,
        config: RateLimitConfigDict | dict[str, Any] | None,
        expected_sleep: float,
    ) -> None:
        """
        Test that non-positive and non-numeric values are ignored and return
        0.0.
        """
        overrides = cast(RateLimitConfigDict | None, config)
        assert (
            RateLimiter.resolve_sleep_seconds(
                rate_limit=rate_limit,
                overrides=overrides,
            )
            == expected_sleep
        )

    def test_overrides_max_per_sec(self) -> None:
        """Test that max_per_sec in config overrides other values."""
        assert (
            RateLimiter.resolve_sleep_seconds(
                rate_limit=None,
                overrides={'max_per_sec': 4},
            )
            == 0.25
        )

    def test_overrides_sleep_seconds(self) -> None:
        """Test that sleep_seconds in config overrides other values."""
        assert (
            RateLimiter.resolve_sleep_seconds(
                rate_limit=None,
                overrides={'sleep_seconds': 0.2},
            )
            == 0.2
        )

    def test_rate_limit_fallback(self) -> None:
        """Test that fallback to rate limit config when override is None."""
        assert (
            RateLimiter.resolve_sleep_seconds(
                rate_limit={'max_per_sec': 2},
                overrides=None,
            )
            == 0.5
        )

    def test_sleep_seconds_precedence(self) -> None:
        """
        Test that ``sleep_seconds`` takes precedence over ``max_per_sec`` when
        both are set.
        """
        assert RateLimiter.resolve_sleep_seconds(
            rate_limit={'sleep_seconds': 0.2, 'max_per_sec': 1},
            overrides=None,
        ) == pytest.approx(0.2)


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

    def test_disabled_constructor(self, disabled_limiter: RateLimiter) -> None:
        """
        Test that disabled() returns a limiter that never sleeps.
        """
        assert disabled_limiter.sleep_seconds == 0.0
        assert disabled_limiter.enabled is False
        assert bool(disabled_limiter) is False
        assert disabled_limiter.max_per_sec is None

    def test_fixed_constructor(
        self,
        fixed_limiter: RateLimiter,
    ) -> None:
        """
        Test that fixed() returns a limiter with the specified positive delay.
        """
        assert fixed_limiter.sleep_seconds == pytest.approx(0.25)
        assert fixed_limiter.enabled is True
        assert bool(fixed_limiter) is True
        assert fixed_limiter.max_per_sec == pytest.approx(4.0)

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
        Test that ``fixed()`` converts inputs to non-negative floats with a
        default of 0.0.
        """
        limiter = RateLimiter.fixed(seconds)  # type: ignore[arg-type]
        assert limiter.sleep_seconds == pytest.approx(expected_sleep)

    def test_post_init_prefers_positive_rate_when_sleep_not_positive(
        self,
    ) -> None:
        """
        Test that positive max_per_sec derives sleep when sleep_seconds <= 0.
        """
        limiter = RateLimiter(sleep_seconds=0.0, max_per_sec=4.0)
        assert limiter.max_per_sec == pytest.approx(4.0)
        assert limiter.sleep_seconds == pytest.approx(0.25)

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

    def test_enforce_calls_sleep_when_enabled(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that ``enforce`` call :func:`time.sleep` with ``sleep_seconds``
        when the limiter is enabled.
        """
        calls: list[float] = []

        def fake_sleep(value: float) -> None:
            calls.append(value)

        # Patch the module-level ``time.sleep`` used by
        # :class:`RateLimiter`.
        monkeypatch.setattr(
            'etlplus.api.rate_limiting._rate_limiter.time.sleep',
            fake_sleep,
        )

        limiter = RateLimiter.fixed(0.5)
        limiter.enforce()

        assert calls == [pytest.approx(0.5)]

    def test_enforce_noop_when_disabled(
        self,
        monkeypatch: pytest.MonkeyPatch,
        disabled_limiter: RateLimiter,
    ) -> None:
        """
        Test that ``enforce`` does not call :func:`time.sleep` when the limiter
        is disabled.
        """
        calls: list[float] = []

        def fake_sleep(value: float) -> None:  # pragma: no cover
            # Should not run.
            calls.append(value)

        monkeypatch.setattr(
            'etlplus.api.rate_limiting._rate_limiter.time.sleep',
            fake_sleep,
        )

        disabled_limiter.enforce()

        assert not calls
