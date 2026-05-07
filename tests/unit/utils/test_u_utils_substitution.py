"""
:mod:`tests.unit.utils.test_u_utils_substitution` module.

Unit tests for :mod:`etlplus.utils._substitution`.
"""

from __future__ import annotations

from typing import Any

import pytest

from etlplus.utils import SubstitutionResolver
from etlplus.utils import TokenReferenceCollector

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
        assert SubstitutionResolver(vars_map_basic).deep(
            {'a': '${FOO}', 'b': 2, 'c': ['${BAR}', 3]},
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
        assert SubstitutionResolver(vars_map_basic, {'FOO': 'envfoo'}).deep(
            {'a': '${FOO}', 'b': '${BAR}'},
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
        assert SubstitutionResolver(vars_map).deep(value) == expected

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
        assert SubstitutionResolver().deep(value) == expected

    def test_nested_container_types(
        self,
        vars_map_nested: dict[str, int],
    ) -> None:
        """
        Test that :meth:`SubstitutionResolver.deep` supports tuple, set, and frozenset
        containers.
        """
        result = SubstitutionResolver(vars_map_nested).deep(
            {
                'a': ['${X}', {'b': '${Y}'}],
                'b': ({'c': '${Z}'},),
                'c': {'${X}', 'x'},
                'd': frozenset({'${Y}', 'y'}),
            },
        )

        assert result == {
            'a': ['1', {'b': '2'}],
            'b': ({'c': '3'},),
            'c': {'1', 'x'},
            'd': frozenset({'2', 'y'}),
        }

    def test_no_substitutions_returns_original_object(self) -> None:
        """Test that no-op substitution avoids unnecessary container copies."""
        value = {'a': ['${MISSING}']}

        assert SubstitutionResolver().deep(value) is value

    def test_resolver_is_frozen(self) -> None:
        """Test that substitution maps cannot be reassigned after construction."""
        resolver = SubstitutionResolver({'FOO': 'foo'})

        with pytest.raises(AttributeError):
            resolver.vars_map = {}  # type: ignore[misc]


class TestTokenReferenceCollector:
    """Unit tests for unresolved token-reference collection."""

    def test_collect_names_walks_nested_container_types(self) -> None:
        """Test token collection across mappings and sequence container types."""
        value = {
            'list': ['${LIST_TOKEN}'],
            'tuple': ('${TUPLE_TOKEN}',),
            'set': {'${SET_TOKEN}'},
            'frozen': frozenset({'${FROZEN_TOKEN}'}),
            'number': 1,
        }

        assert TokenReferenceCollector.collect_names(value) == {
            'FROZEN_TOKEN',
            'LIST_TOKEN',
            'SET_TOKEN',
            'TUPLE_TOKEN',
        }

    def test_collect_rows_returns_stable_paths(self) -> None:
        """Test token row collection includes sorted reference paths."""
        assert TokenReferenceCollector.collect_rows(
            {
                'root': '${ROOT}',
                'nested': [{'value': '${ROOT}'}, '${OTHER}'],
            },
        ) == [
            {'name': 'OTHER', 'paths': ['nested[1]']},
            {'name': 'ROOT', 'paths': ['nested[0].value', 'root']},
        ]

    def test_collect_rows_supports_custom_pattern(self) -> None:
        """Test custom token patterns can be injected by callers."""
        import re

        assert TokenReferenceCollector.collect_rows(
            {'path': 'Hello {{NAME}}'},
            pattern=re.compile(r'\{\{([^}]+)\}\}'),
        ) == [{'name': 'NAME', 'paths': ['path']}]
