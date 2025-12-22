"""
:mod:`tests.unit.api.test_u_rate_limit_config` module.

Unit tests for :class:`etlplus.api.rate_limiting.RateLimitConfig`.

Notes
-----
- Ensures graceful handling of non-mapping inputs and type coercion for
    rate limit configuration.
"""

from __future__ import annotations

from collections.abc import Callable
from collections.abc import Iterable

import pytest

from etlplus.api import RateLimitConfig

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.unit

# SECTION: TESTS ============================================================ #


@pytest.mark.unit
class TestRateLimitConfig:
    """
    Unit test suite for :class:`RateLimitConfig`.

    Notes
    -----
    Tests equality semantics and type coercion for rate limit configuration.
    """

    def test_config_honors_overrides(self) -> None:
        """Overrides replace base config values."""
        config = RateLimitConfig.from_inputs(
            rate_limit={'max_per_sec': 2},
            overrides={'sleep_seconds': 0.1},
        )
        assert config.sleep_seconds == pytest.approx(0.1)
        assert config.max_per_sec == pytest.approx(10.0)

    def test_config_prefers_sleep_seconds(self) -> None:
        """Sleep seconds take precedence over max_per_sec."""
        config = RateLimitConfig.from_inputs(
            rate_limit={'sleep_seconds': 0.2, 'max_per_sec': 1},
        )
        assert config.enabled is True
        assert config.sleep_seconds == pytest.approx(0.2)
        assert config.max_per_sec == pytest.approx(5.0)

    def test_equality_semantics(
        self,
        rate_limit_config_factory: Callable[..., RateLimitConfig],
    ) -> None:
        """
        Test equality semantics for RateLimitConfig instances.

        Parameters
        ----------
        rate_limit_config_factory : Callable[..., RateLimitConfig]
            Factory for creating RateLimitConfig instances.
        """
        a = rate_limit_config_factory(sleep_seconds=1.0, max_per_sec=3.0)
        b = rate_limit_config_factory(sleep_seconds=1.0, max_per_sec=3.0)
        c = rate_limit_config_factory(sleep_seconds=None, max_per_sec=3.0)
        assert a == b
        assert a != c

    @pytest.mark.parametrize(
        'obj,expect',
        [
            pytest.param(
                {'sleep_seconds': '0.25', 'max_per_sec': '2'},
                (0.25, 2.0),
                id='coerces-str-numerics',
            ),
            pytest.param(
                {'sleep_seconds': 'x', 'max_per_sec': None},
                (None, None),
                id='ignores-bad-values',
            ),
        ],
    )
    def test_from_obj_coercion(
        self,
        rate_limit_from_obj_factory: Callable[[dict], RateLimitConfig],
        obj: dict,
        expect: tuple,
    ) -> None:
        """
        Test that RateLimitConfig correctly coerces types from input objects.

        Parameters
        ----------
        rate_limit_from_obj_factory : Callable[[dict], RateLimitConfig]
            Factory for creating RateLimitConfig from dicts.
        obj : dict
            Input dictionary for rate limit configuration.
        expect : tuple
            Expected (sleep_seconds, max_per_sec) values.
        """
        rl = rate_limit_from_obj_factory(obj)
        assert (rl.sleep_seconds, rl.max_per_sec) == expect

    def test_from_obj_non_mapping_iterable_returns_none(
        self,
        rate_limit_from_obj_factory: Callable[[object], object],
    ) -> None:
        """
        Test that from_obj returns None for non-mapping iterable inputs.

        Parameters
        ----------
        rate_limit_from_obj_factory : Callable[[object], object]
            Factory for creating RateLimitConfig from mapping or iterable.
        """

        class Weird(Iterable):
            """A weird iterable that is not a mapping."""

            def __iter__(self):  # type: ignore[override]
                yield 'sleep_seconds'
                yield '1'

        # from_obj should return None (not raise) for non-mapping inputs.
        res = rate_limit_from_obj_factory(Weird())  # type: ignore[arg-type]
        assert res is None

    def test_no_side_effects_on_input_mapping(
        self,
        rate_limit_from_obj_factory: Callable[[dict[str, str]], object],
    ) -> None:
        """
        Test that input mapping is not mutated by from_obj (defensive copy).

        Parameters
        ----------
        rate_limit_from_obj_factory : Callable[[dict[str, str]], object]
            Factory for creating RateLimitConfig from dict.
        """
        obj = {'sleep_seconds': '1', 'max_per_sec': '3'}
        _ = rate_limit_from_obj_factory(obj)
        # Original mapping should remain unchanged (defensive copy behavior).
        assert obj == {'sleep_seconds': '1', 'max_per_sec': '3'}

    def test_repr_roundtrip(
        self,
        rate_limit_config_factory: Callable[..., RateLimitConfig],
    ) -> None:
        """
        Test that repr output includes field names and values.

        Parameters
        ----------
        rate_limit_config_factory : Callable[..., RateLimitConfig]
            Factory for creating RateLimitConfig instances.
        """
        rl = rate_limit_config_factory(sleep_seconds=0.5, max_per_sec=2.0)
        # Best-effort: repr should mention field names & values.
        r = repr(rl)
        for frag in ('sleep_seconds', 'max_per_sec', '0.5', '2.0'):
            assert frag in r

    def test_unhashable_dataclass(
        self,
        rate_limit_config_factory: Callable[..., RateLimitConfig],
    ) -> None:
        """
        Test that RateLimitConfig dataclass is unhashable.

        Parameters
        ----------
        rate_limit_config_factory : Callable[..., RateLimitConfig]
            Factory for creating RateLimitConfig instances.
        """
        rl = rate_limit_config_factory(sleep_seconds=1.0, max_per_sec=3.0)
        # dataclass(slots=True) without frozen=True should be unhashable.
        with pytest.raises(TypeError):
            hash(rl)

    def test_validate_bounds_contains_only_known_messages(
        self,
        rate_limit_config_factory: Callable[..., RateLimitConfig],
    ) -> None:
        """
        Test that validate_bounds only returns known warning messages.

        Parameters
        ----------
        rate_limit_config_factory : Callable[..., RateLimitConfig]
            Factory for creating RateLimitConfig instances.
        """
        rl = rate_limit_config_factory(sleep_seconds=-5, max_per_sec=-1)
        allowed = {
            'sleep_seconds should be >= 0',
            'max_per_sec should be > 0',
        }
        warnings = rl.validate_bounds()
        # Ensure every warning is recognized and no extras appear.
        assert warnings and set(warnings).issubset(allowed)

    @pytest.mark.parametrize(
        'kwargs,expected_warnings',
        [
            pytest.param(
                {'sleep_seconds': 0.0, 'max_per_sec': 1.5},
                [],
                id='valid-no-warnings',
            ),
            pytest.param(
                {'sleep_seconds': -0.1, 'max_per_sec': 0.0},
                ['sleep_seconds should be >= 0', 'max_per_sec should be > 0'],
                id='invalid-both-out-of-range',
            ),
        ],
    )
    def test_validate_bounds_param(
        self,
        rate_limit_config_factory: Callable[..., RateLimitConfig],
        kwargs: dict[str, float],
        expected_warnings: list[str],
    ) -> None:
        """
        Test that validate_bounds returns expected warnings for given params.

        Parameters
        ----------
        rate_limit_config_factory : Callable[..., RateLimitConfig]
            Factory for creating RateLimitConfig instances.
        kwargs : dict[str, float]
            Keyword arguments for RateLimitConfig.
        expected_warnings : list[str]
            Expected warning messages.
        """
        rl = rate_limit_config_factory(**kwargs)
        warnings = rl.validate_bounds()
        # Order not guaranteed; compare as sets.
        assert set(warnings) == set(expected_warnings)
