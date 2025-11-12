"""
``tests.unit.api.test_u_rate`` module.

Unit tests for ``etlplus.api.rate``.

Notes
-----
- Ensures non-positive and non-numeric inputs result in 0.0 seconds.
"""
from __future__ import annotations

from etlplus.api.rate import compute_sleep_seconds


# SECTION: TESTS ============================================================ #


def test_compute_sleep_seconds_overrides_sleep_seconds() -> None:
    assert compute_sleep_seconds(None, {'sleep_seconds': 0.2}) == 0.2


def test_compute_sleep_seconds_overrides_max_per_sec() -> None:
    assert compute_sleep_seconds(None, {'max_per_sec': 4}) == 0.25


def test_compute_sleep_seconds_rate_limit_fallback() -> None:
    assert compute_sleep_seconds({'max_per_sec': 2}, None) == 0.5


def test_compute_sleep_seconds_invalid_values() -> None:
    # Non-positive and non-numeric values are ignored -> 0.0
    assert compute_sleep_seconds({'sleep_seconds': -1}, None) == 0.0
    assert compute_sleep_seconds(None, {'max_per_sec': 'oops'}) == 0.0
