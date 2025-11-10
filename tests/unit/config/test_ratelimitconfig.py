"""
tests.unit.config.test_ratelimitconfig unit tests module.


Ensure the optional validate_bounds() helpers return non-fatal warnings
for out-of-range numeric parameters.
"""
from __future__ import annotations


# SECTION: TESTS ============================================================ #


class TestRateLimitConfig:
    def test_valid_values_no_warnings(
        self,
        rate_limit_config_factory,
    ) -> None:  # noqa: D401
        rl = rate_limit_config_factory(sleep_seconds=0.0, max_per_sec=1.5)
        assert rl.validate_bounds() == []

    def test_validate_bounds(
        self,
        rate_limit_config_factory,
    ) -> None:  # noqa: D401
        rl = rate_limit_config_factory(sleep_seconds=-0.1, max_per_sec=0.0)
        warnings = rl.validate_bounds()
        assert 'sleep_seconds should be >= 0' in warnings
        assert 'max_per_sec should be > 0' in warnings

    def test_from_obj_coerces_numeric_fields(
        self,
        rate_limit_from_obj_factory,
    ) -> None:  # noqa: D401
        obj = {'sleep_seconds': '0.25', 'max_per_sec': '2'}
        rl = rate_limit_from_obj_factory(obj)
        assert rl is not None
        assert rl.sleep_seconds == 0.25
        assert rl.max_per_sec == 2.0

    def test_from_obj_ignores_bad_numeric_values(
        self,
        rate_limit_from_obj_factory,
    ) -> None:  # noqa: D401
        obj = {'sleep_seconds': 'x', 'max_per_sec': None}
        rl = rate_limit_from_obj_factory(obj)
        assert rl is not None
        assert rl.sleep_seconds is None
        assert rl.max_per_sec is None
