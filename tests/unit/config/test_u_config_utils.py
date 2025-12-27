"""
:mod:`tests.unit.config.test_u_config_utils` module.

Unit tests for :class:`etlplus.config.utils`.

Covers deep_substitute and edge cases.
"""

import pytest

from etlplus.config import utils as config_utils

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.unit

# SECTION: TESTS ============================================================ #


def test_deep_substitute_basic():
    """Test basic deep substitution functionality."""
    value = {'a': '${FOO}', 'b': 2, 'c': ['${BAR}', 3]}
    vars_map = {'FOO': 'foo', 'BAR': 'bar'}
    result = config_utils.deep_substitute(value, vars_map, None)
    assert result == {'a': 'foo', 'b': 2, 'c': ['bar', 3]}


def test_deep_substitute_empty():
    """Test deep substitution with empty structures."""
    assert config_utils.deep_substitute('', None, None) == ''
    assert config_utils.deep_substitute({}, None, None) == {}
    assert config_utils.deep_substitute([], None, None) == []
    assert config_utils.deep_substitute(None, None, None) is None


def test_deep_substitute_env_override():
    """Test that environment variables override vars_map in substitution."""
    value = {'a': '${FOO}', 'b': '${BAR}'}
    vars_map = {'FOO': 'foo', 'BAR': 'bar'}
    env_map = {'FOO': 'envfoo'}
    result = config_utils.deep_substitute(value, vars_map, env_map)
    assert result['a'] == 'envfoo'
    assert result['b'] == 'bar'


def test_deep_substitute_nested_structures():
    """Test deep substitution in nested structures."""
    value = {'a': ['${X}', {'b': '${Y}'}], 'c': ({'d': '${Z}'},)}
    vars_map = {'X': 1, 'Y': 2, 'Z': 3}
    result = config_utils.deep_substitute(value, vars_map, None)
    assert result == {'a': ['1', {'b': '2'}], 'c': ({'d': '3'},)}


def test_deep_substitute_no_subs():
    """Test deep substitution when no substitutions are needed."""
    value = {'a': 1, 'b': [2, 3], 'c': {'d': 4}}
    result = config_utils.deep_substitute(value, None, None)
    assert result == value


def test_deep_substitute_sets():
    """Test deep substitution within set and frozenset structures."""
    value = {'a': {'${FOO}', 'bar'}, 'b': frozenset(['${FOO}', 'baz'])}
    vars_map = {'FOO': 'f'}
    result = config_utils.deep_substitute(value, vars_map, None)
    assert 'f' in result['a'] and 'bar' in result['a']
    assert 'f' in result['b'] and 'baz' in result['b']


def test_deep_substitute_token_not_found():
    """
    Test behavior when a token is not found in ``vars_map`` or ``env_map.``
    """
    value = 'Hello ${MISSING}'
    result = config_utils.deep_substitute(value, {'FOO': 'foo'}, None)
    assert result == 'Hello ${MISSING}'
