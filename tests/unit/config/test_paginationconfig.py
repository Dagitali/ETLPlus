"""
Bounds Validation Tests
=======================

Ensure the optional validate_bounds() helpers return non-fatal warnings
for out-of-range numeric parameters.
"""
from __future__ import annotations

from typing import Literal

import pytest

from etlplus.config import PaginationConfig


@pytest.mark.parametrize(
    'tval',
    [None, 'weird', ''],
    ids=['none', 'weird', 'empty'],
)
def test_pagination_unknown_type_general_warnings_only(tval):
    pc = PaginationConfig(
        type=tval,
        start_page=0,
        page_size=0,
        max_pages=0,
        max_records=-1,
    )
    warnings = pc.validate_bounds()

    # General warnings should be present
    assert 'max_pages should be > 0' in warnings
    assert 'max_records should be > 0' in warnings

    # No page/offset or cursor-specific warnings for unknown types
    assert not any('start_page should be >= 1' in w for w in warnings)
    assert not any(
        'page_size should be > 0 for cursor pagination' in w
        for w in warnings
    )
    assert not any('page_size should be > 0' in w for w in warnings)

    pc = PaginationConfig(type='offset', start_page=0, page_size=-1)
    warnings = pc.validate_bounds()
    assert 'start_page should be >= 1' in warnings
    assert 'page_size should be > 0' in warnings


def test_pagination_valid_values_no_warnings():
    pc = PaginationConfig(
        type='page',
        start_page=1,
        page_size=10,
        max_pages=5,
        max_records=100,
    )
    assert pc.validate_bounds() == []


@pytest.mark.parametrize(
    'ptype',
    ['page', 'offset', 'cursor'],
    ids=['page', 'offset', 'cursor'],
)
def test_pagination_validate_bounds_parametrized(
    ptype: Literal['page', 'offset', 'cursor'],
):
    pc = PaginationConfig(
        type=ptype,
        start_page=0,
        page_size=0,
        max_pages=0,
        max_records=-1,
    )
    warnings = pc.validate_bounds()

    # General warnings should always appear
    assert 'max_pages should be > 0' in warnings
    assert 'max_records should be > 0' in warnings

    if ptype in {'page', 'offset'}:
        assert 'start_page should be >= 1' in warnings
        assert 'page_size should be > 0' in warnings
        assert not any(
            'page_size should be > 0 for cursor pagination' in w
            for w in warnings
        )
    else:  # cursor
        assert not any('start_page should be >= 1' in w for w in warnings)
        assert any(
            'page_size should be > 0 for cursor pagination' in w
            for w in warnings
        )


def test_pagination_offset_mode_warnings():
    pc = PaginationConfig(type='offset', start_page=0, page_size=-1)
    warnings = pc.validate_bounds()
    assert 'start_page should be >= 1' in warnings
    assert 'page_size should be > 0' in warnings


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
