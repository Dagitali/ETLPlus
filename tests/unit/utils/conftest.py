"""
:mod:`tests.unit.utils.conftest` module.

Shared fixtures and helpers for pytest-based unit tests of
:mod:`etlplus.utils` modules.
"""

from __future__ import annotations

from typing import Any

import pytest

# SECTION: MARKERS ========================================================== #


# Directory-level marker for unit tests.
pytestmark = pytest.mark.unit


# SECTION: FIXTURES ========================================================= #


@pytest.fixture(
    name='non_mapping_value',
    params=[
        pytest.param(None, id='none'),
        pytest.param('abc', id='string'),
        pytest.param('', id='empty-string'),
        pytest.param([1, 2], id='list'),
        pytest.param((1, 2), id='tuple'),
        pytest.param(True, id='bool'),
    ],
)
def non_mapping_value_fixture(
    request: pytest.FixtureRequest,
) -> Any:
    """
    Return representative non-mapping values used in multiple test modules.
    """
    return request.param
