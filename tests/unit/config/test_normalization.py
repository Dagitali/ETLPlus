"""
Normalization Tests for Config Parsers
=====================================

Covers numeric coercion and tolerant parsing in PaginationConfig and
RateLimitConfig.
"""
from __future__ import annotations

from etlplus.config import PaginationConfig
from etlplus.config import RateLimitConfig


def test_pagination_from_obj_coerces_numeric_fields():
    obj = {
        'type': 'page',
        'page_param': 'page',
        'size_param': 'per_page',
        'start_page': '1',
        'page_size': '50',
        'records_path': 'data.items',
        'max_pages': '10',
        'max_records': '1000',
    }
    pc = PaginationConfig.from_obj(obj)
    assert pc is not None
    assert pc.type == 'page'
    assert pc.start_page == 1
    assert pc.page_size == 50
    assert pc.max_pages == 10
    assert pc.max_records == 1000


def test_pagination_from_obj_ignores_bad_numeric_values():
    obj = {
        'type': 'page',
        'start_page': 'not-an-int',
        'page_size': None,
        'max_pages': [],
        'max_records': {},
    }
    pc = PaginationConfig.from_obj(obj)
    assert pc is not None
    assert pc.start_page is None
    assert pc.page_size is None
    assert pc.max_pages is None
    assert pc.max_records is None


def test_rate_limit_from_obj_coerces_numeric_fields():
    obj = {'sleep_seconds': '0.25', 'max_per_sec': '2'}
    rl = RateLimitConfig.from_obj(obj)
    assert rl is not None
    assert rl.sleep_seconds == 0.25
    assert rl.max_per_sec == 2.0


def test_rate_limit_from_obj_ignores_bad_numeric_values():
    obj = {'sleep_seconds': 'x', 'max_per_sec': None}
    rl = RateLimitConfig.from_obj(obj)
    assert rl is not None
    assert rl.sleep_seconds is None
    assert rl.max_per_sec is None
