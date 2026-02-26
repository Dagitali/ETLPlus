"""
:mod:`tests.unit.utils.test_u_utils_substitution` module.

Unit tests for :mod:`etlplus.utils.substitution`.
"""

from __future__ import annotations

import pytest

from etlplus.utils import deep_substitute

# SECTION: FIXTURES ========================================================= #


@pytest.fixture(name='vars_map_basic')
def vars_map_basic_fixture() -> dict[str, str]:
    """Provide a basic variables mapping for token substitution."""

    return {'FOO': 'foo', 'BAR': 'bar'}


@pytest.fixture(name='vars_map_nested')
def vars_map_nested_fixture() -> dict[str, int]:
    """Provide an integer variables mapping used in nested substitutions."""

    return {'X': 1, 'Y': 2, 'Z': 3}


# SECTION: TESTS ============================================================ #


class TestSubstitution:
    """Unit tests for :func:`etlplus.utils.deep_substitution`."""

    def test_basic_substitution(self, vars_map_basic: dict[str, str]) -> None:
        """Test substituting tokens across nested mappings and sequences."""

        value = {'a': '${FOO}', 'b': 2, 'c': ['${BAR}', 3]}
        result = deep_substitute(value, vars_map_basic, None)

        assert result == {'a': 'foo', 'b': 2, 'c': ['bar', 3]}

    @pytest.mark.parametrize(
        'value, expected',
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
        """Test that empty inputs are returned unchanged."""

        result = deep_substitute(value, None, None)
        if expected is None:
            assert result is None
        else:
            assert result == expected

    def test_env_overrides_vars_map(
        self,
        vars_map_basic: dict[str, str],
    ) -> None:
        """
        Test that ``env_map`` values are preferred over ``vars_map`` values.
        """

        value = {'a': '${FOO}', 'b': '${BAR}'}
        env_map = {'FOO': 'envfoo'}

        result = deep_substitute(value, vars_map_basic, env_map)

        assert result == {'a': 'envfoo', 'b': 'bar'}

    def test_nested_structures(self, vars_map_nested: dict[str, int]) -> None:
        """Test substituting tokens in nested structures, including tuples."""

        value = {'a': ['${X}', {'b': '${Y}'}], 'c': ({'d': '${Z}'},)}
        result = deep_substitute(value, vars_map_nested, None)

        # deep_substitute coerces substituted values to strings.
        assert result == {'a': ['1', {'b': '2'}], 'c': ({'d': '3'},)}

    def test_no_substitutions_needed(self) -> None:
        """
        Test returning the original value when no substitutions are required.
        """

        value = {'a': 1, 'b': [2, 3], 'c': {'d': 4}}
        result = deep_substitute(value, None, None)

        assert result == value

    def test_sets_and_frozensets(self) -> None:
        """Test substituting tokens within set-like container structures."""

        value = {'a': {'${FOO}', 'bar'}, 'b': frozenset(['${FOO}', 'baz'])}
        result = deep_substitute(value, {'FOO': 'f'}, None)

        assert result['a'] == {'f', 'bar'}
        assert result['b'] == frozenset({'f', 'baz'})

    def test_token_not_found_returns_original(self) -> None:
        """
        Test that unknown tokens are left unchanged when no mapping provides a
        value.
        """

        value = 'Hello ${MISSING}'
        result = deep_substitute(value, {'FOO': 'foo'}, None)

        assert result == 'Hello ${MISSING}'
