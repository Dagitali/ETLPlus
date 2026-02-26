"""
:mod:`tests.unit.utils.conftest` module.

Shared fixtures and helpers for pytest-based unit tests of
:mod:`etlplus.utils` modules.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

import pytest

# SECTION: MARKERS ========================================================== #


# Directory-level marker for unit tests.
pytestmark = pytest.mark.unit


# SECTION: TYPE ALIASES ===================================================== #


type JsonOutputAsserter = Callable[[str, object], None]


# SECTION: FIXTURES ========================================================= #


@pytest.fixture(name='assert_json_output')
def assert_json_output_fixture() -> JsonOutputAsserter:
    """Return an assertion helper that validates printed JSON output."""

    def _assert_json_output(
        output: str,
        expected: object,
    ) -> None:
        assert json.loads(output) == expected

    return _assert_json_output


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


@pytest.fixture(name='unicode_payload')
def unicode_payload_fixture() -> dict[str, str]:
    """Return a payload containing non-ASCII text for JSON print tests."""
    return {'emoji': '\u2603'}


@pytest.fixture(name='vars_map_basic')
def vars_map_basic_fixture() -> dict[str, str]:
    """Return a basic variables mapping used for substitution tests."""
    return {'FOO': 'foo', 'BAR': 'bar'}


@pytest.fixture(name='vars_map_nested')
def vars_map_nested_fixture() -> dict[str, int]:
    """
    Return an integer variables mapping used for nested substitution tests.
    """
    return {'X': 1, 'Y': 2, 'Z': 3}
