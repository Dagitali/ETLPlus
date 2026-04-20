"""
:mod:`tests.unit.utils.test_u_utils_substitution` module.

Unit tests for :mod:`etlplus.utils._substitution`.
"""

from __future__ import annotations

from typing import Any

import pytest

from etlplus.utils import SubstitutionResolver

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestDeepSubstitute:
    """Unit tests for recursive substitution helpers."""

    def test_basic_nested_substitution(
        self,
        vars_map_basic: dict[str, str],
    ) -> None:
        """
        Test that :meth:`SubstitutionResolver.deep` recurses through nested
        mappings and sequences to replace tokens with values from the provided
        maps.
        """
        assert SubstitutionResolver.deep(
            {'a': '${FOO}', 'b': 2, 'c': ['${BAR}', 3]},
            vars_map_basic,
            None,
        ) == {
            'a': 'foo',
            'b': 2,
            'c': ['bar', 3],
        }

    def test_env_overrides_vars(
        self,
        vars_map_basic: dict[str, str],
    ) -> None:
        """
        Test that :meth:`SubstitutionResolver.deep` environment values take
        precedence over vars-map values when keys overlap.
        """
        assert SubstitutionResolver.deep(
            {'a': '${FOO}', 'b': '${BAR}'},
            vars_map_basic,
            {'FOO': 'envfoo'},
        ) == {
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
    def test_case_matrix(
        self,
        value: Any,
        vars_map: dict[str, object] | None,
        expected: Any,
    ) -> None:
        """
        Test that :meth:`SubstitutionResolver.deep` applies substitutions
        recursively and preserves non-string values while replacing tokens.
        """
        assert SubstitutionResolver.deep(value, vars_map, None) == expected

    @pytest.mark.parametrize(
        ('value', 'expected'),
        [
            pytest.param('', '', id='empty-string'),
            pytest.param({}, {}, id='empty-dict'),
            pytest.param([], [], id='empty-list'),
            pytest.param(None, None, id='none'),
        ],
    )
    def test_empty_inputs_passthrough(
        self,
        value: object,
        expected: object,
    ) -> None:
        """
        Test that :meth:`SubstitutionResolver.deep` preserves empty and
        ``None`` inputs when maps are missing.
        """
        assert SubstitutionResolver.deep(value, None, None) == expected

    def test_nested_container_types(
        self,
        vars_map_nested: dict[str, int],
    ) -> None:
        """
        Test that :meth:`SubstitutionResolver.deep` supports tuple, set, and frozenset
        containers.
        """
        result = SubstitutionResolver.deep(
            {
                'a': ['${X}', {'b': '${Y}'}],
                'b': ({'c': '${Z}'},),
                'c': {'${X}', 'x'},
                'd': frozenset({'${Y}', 'y'}),
            },
            vars_map_nested,
            None,
        )

        assert result == {
            'a': ['1', {'b': '2'}],
            'b': ({'c': '3'},),
            'c': {'1', 'x'},
            'd': frozenset({'2', 'y'}),
        }
