"""
Bounds Validation Tests
=======================

Ensure the optional validate_bounds() helpers return non-fatal warnings
for out-of-range numeric parameters.
"""
from __future__ import annotations

from etlplus.config import PaginationConfig
from etlplus.config import RateLimitConfig


def test_pagination_validate_bounds_page_mode():
    pc = PaginationConfig(
        type='page', start_page=0, page_size=0, max_pages=0, max_records=-1,
    )
    warnings = pc.validate_bounds()
    assert 'start_page should be >= 1' in warnings
    assert 'page_size should be > 0' in warnings
    assert 'max_pages should be > 0' in warnings
    assert 'max_records should be > 0' in warnings


def test_pagination_validate_bounds_cursor_mode():
    pc = PaginationConfig(type='cursor', page_size=0)
    warnings = pc.validate_bounds()
    assert any('page_size should be > 0' in w for w in warnings)


def test_rate_limit_validate_bounds():
    rl = RateLimitConfig(sleep_seconds=-0.1, max_per_sec=0.0)
    warnings = rl.validate_bounds()
    assert 'sleep_seconds should be >= 0' in warnings
    assert 'max_per_sec should be > 0' in warnings
