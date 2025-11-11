"""
tests.unit.config.test_ratelimitconfig unit tests module.

Unit tests for :class:`RateLimitConfig`.

Ensure the optional validate_bounds() helpers return non-fatal warnings
for out-of-range numeric parameters.

Focus: constructor / from_obj coercion + non-fatal bounds validation.
"""
from __future__ import annotations

from collections.abc import Iterable
from typing import Any  # noqa: F401  # reserved for future extensions

import pytest

from etlplus.config import RateLimitConfig


# SECTION: TESTS ============================================================ #


class TestRateLimitConfig:
    def test_equality_semantics(
        self,
        rate_limit_config_factory,  # type: ignore[no-untyped-def]
    ):
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
        # type: ignore[no-untyped-def]
        rate_limit_from_obj_factory, obj, expect,
    ):
        rl: RateLimitConfig = rate_limit_from_obj_factory(obj)
        assert (rl.sleep_seconds, rl.max_per_sec) == expect

    def test_from_obj_non_mapping_iterable_returns_none(
        self,
        rate_limit_from_obj_factory,  # type: ignore[no-untyped-def]
    ):
        class Weird(Iterable):  # noqa: D401
            def __iter__(self):  # type: ignore[override]
                yield 'sleep_seconds'
                yield '1'

        # from_obj should return None (not raise) for non-mapping inputs
        res = rate_limit_from_obj_factory(Weird())  # type: ignore[arg-type]
        assert res is None

    def test_no_side_effects_on_input_mapping(
        self,
        rate_limit_from_obj_factory,  # type: ignore[no-untyped-def]
    ):
        obj = {'sleep_seconds': '1', 'max_per_sec': '3'}
        _ = rate_limit_from_obj_factory(obj)
        # Original mapping should remain unchanged (defensive copy behavior)
        assert obj == {'sleep_seconds': '1', 'max_per_sec': '3'}

    def test_repr_roundtrip(
        self,
        rate_limit_config_factory,  # type: ignore[no-untyped-def]
    ):
        rl = rate_limit_config_factory(sleep_seconds=0.5, max_per_sec=2.0)
        # Best-effort: repr should mention field names & values
        r = repr(rl)
        for frag in ('sleep_seconds', 'max_per_sec', '0.5', '2.0'):
            assert frag in r

    def test_unhashable_dataclass(
        self,
        rate_limit_config_factory,  # type: ignore[no-untyped-def]
    ):
        rl = rate_limit_config_factory(sleep_seconds=1.0, max_per_sec=3.0)
        # dataclass(slots=True) without frozen=True should be unhashable
        with pytest.raises(TypeError):
            hash(rl)

    def test_validate_bounds_contains_only_known_messages(
        self,
        rate_limit_config_factory,  # type: ignore[no-untyped-def]
    ):
        rl = rate_limit_config_factory(sleep_seconds=-5, max_per_sec=-1)
        allowed = {
            'sleep_seconds should be >= 0',
            'max_per_sec should be > 0',
        }
        warnings = rl.validate_bounds()
        # Ensure every warning is recognized and no extras appear
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
        rate_limit_config_factory,
        kwargs,
        expected_warnings,  # type: ignore[no-untyped-def]
    ):
        rl: RateLimitConfig = rate_limit_config_factory(**kwargs)
        warnings = rl.validate_bounds()
        # Order not guaranteed; compare as sets
        assert set(warnings) == set(expected_warnings)
