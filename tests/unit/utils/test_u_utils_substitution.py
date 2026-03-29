"""
:mod:`tests.unit.utils.test_u_utils_substitution` module.

Unit tests for :mod:`etlplus.utils._substitution`.
"""

from __future__ import annotations

from typing import Any

import pytest

from etlplus.utils import deep_substitute

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


def test_deep_substitute_basic_nested_substitution(
    vars_map_basic: dict[str, str],
) -> None:
    """
    Test that :func:`deep_substitute` substitutes tokens recursively in nested
    mappings and sequences.
    """
    value = {'a': '${FOO}', 'b': 2, 'c': ['${BAR}', 3]}
    assert deep_substitute(value, vars_map_basic, None) == {
        'a': 'foo',
        'b': 2,
        'c': ['bar', 3],
    }


def test_deep_substitute_env_overrides_vars(
    vars_map_basic: dict[str, str],
) -> None:
    """
    Test that :func:`deep_substitute` prefers ``env_map`` values over
    ``vars_map`` when keys overlap.
    """
    value = {'a': '${FOO}', 'b': '${BAR}'}
    env_map = {'FOO': 'envfoo'}
    assert deep_substitute(value, vars_map_basic, env_map) == {
        'a': 'envfoo',
        'b': 'bar',
    }


@pytest.mark.parametrize(
    ('value', 'vars_map', 'expected'),
    [
        pytest.param(
            'Hello ${MISSING}',
            {'FOO': 'foo'},
            'Hello ${MISSING}',
            id='missing-token-left-intact',
        ),
        pytest.param(
            {'a': 1, 'b': [2, 3], 'c': {'d': 4}},
            None,
            {'a': 1, 'b': [2, 3], 'c': {'d': 4}},
            id='no-substitutions-needed',
        ),
    ],
)
def test_deep_substitute_case_matrix(
    value: Any,
    vars_map: dict[str, object] | None,
    expected: Any,
) -> None:
    """
    Test that :func:`deep_substitute` applies substitutions recursively and
    preserves non-string values.
    """
    assert deep_substitute(value, vars_map, None) == expected


@pytest.mark.parametrize(
    ('value', 'expected'),
    [
        pytest.param('', '', id='empty-string'),
        pytest.param({}, {}, id='empty-dict'),
        pytest.param([], [], id='empty-list'),
        pytest.param(None, None, id='none'),
    ],
)
def test_deep_substitute_empty_inputs_passthrough(
    value: object,
    expected: object,
) -> None:
    """
    Test that :func:`deep_substitute` returns empty/``None`` values unchanged
    when maps are missing.
    """
    assert deep_substitute(value, None, None) == expected


def test_deep_substitute_nested_container_types(
    vars_map_nested: dict[str, int],
) -> None:
    """
    Test that :func:`deep_substitute` supports nested tuple/set/frozenset
    containers while substituting.
    """
    value = {
        'a': ['${X}', {'b': '${Y}'}],
        'b': ({'c': '${Z}'},),
        'c': {'${X}', 'x'},
        'd': frozenset({'${Y}', 'y'}),
    }
    result = deep_substitute(value, vars_map_nested, None)
    assert result == {
        'a': ['1', {'b': '2'}],
        'b': ({'c': '3'},),
        'c': {'1', 'x'},
        'd': frozenset({'2', 'y'}),
    }
