"""
:mod:`tests.unit.test_u_utils` module.

Unit tests for :mod:`etlplus.utils`.

Notes
-----
- Unit tests for shared numeric coercion helpers.
"""

from __future__ import annotations

import pytest

from etlplus import utils as config_utils
from etlplus.utils import cast_str_dict
from etlplus.utils import coerce_dict
from etlplus.utils import count_records
from etlplus.utils import maybe_mapping
from etlplus.utils import normalize_str
from etlplus.utils import print_json
from etlplus.utils import to_float
from etlplus.utils import to_int
from etlplus.utils import to_maximum_float
from etlplus.utils import to_maximum_int
from etlplus.utils import to_minimum_float
from etlplus.utils import to_minimum_int
from etlplus.utils import to_number
from etlplus.utils import to_positive_float
from etlplus.utils import to_positive_int

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.unit


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


class TestDeepSubstitute:
    """Unit test suite for :func:`etlplus.utils.deep_substitute`."""

    def test_basic_substitution(self, vars_map_basic: dict[str, str]) -> None:
        """Test substituting tokens across nested mappings and sequences."""

        value = {'a': '${FOO}', 'b': 2, 'c': ['${BAR}', 3]}
        result = config_utils.deep_substitute(value, vars_map_basic, None)

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

        result = config_utils.deep_substitute(value, None, None)
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

        result = config_utils.deep_substitute(value, vars_map_basic, env_map)

        assert result == {'a': 'envfoo', 'b': 'bar'}

    def test_nested_structures(self, vars_map_nested: dict[str, int]) -> None:
        """Test substituting tokens in nested structures, including tuples."""

        value = {'a': ['${X}', {'b': '${Y}'}], 'c': ({'d': '${Z}'},)}
        result = config_utils.deep_substitute(value, vars_map_nested, None)

        # deep_substitute coerces substituted values to strings.
        assert result == {'a': ['1', {'b': '2'}], 'c': ({'d': '3'},)}

    def test_no_substitutions_needed(self) -> None:
        """
        Test returning the original value when no substitutions are required.
        """

        value = {'a': 1, 'b': [2, 3], 'c': {'d': 4}}
        result = config_utils.deep_substitute(value, None, None)

        assert result == value

    def test_sets_and_frozensets(self) -> None:
        """Test substituting tokens within set-like container structures."""

        value = {'a': {'${FOO}', 'bar'}, 'b': frozenset(['${FOO}', 'baz'])}
        result = config_utils.deep_substitute(value, {'FOO': 'f'}, None)

        assert result['a'] == {'f', 'bar'}
        assert result['b'] == frozenset({'f', 'baz'})

    def test_token_not_found_returns_original(self) -> None:
        """
        Test that unknown tokens are left unchanged when no mapping provides a
        value.
        """

        value = 'Hello ${MISSING}'
        result = config_utils.deep_substitute(value, {'FOO': 'foo'}, None)

        assert result == 'Hello ${MISSING}'


@pytest.mark.unit
class TestUtils:
    """
    Unit test suite for ``etlplus.utils``.

    Notes
    -----
    - Validates shared numeric coercion helpers.

    Examples
    --------
    >>> to_float('2.5')
    2.5
    >>> to_int('10')
    10
    >>> to_number('3.14')
    3.14
    """

    def test_cast_and_coerce_dict_helpers(self) -> None:
        """Test mapping helpers that normalize dictionaries."""

        assert cast_str_dict(None) == {}
        assert cast_str_dict({'a': 1}) == {'a': '1'}
        assert coerce_dict({'k': 'v'}) == {'k': 'v'}
        assert not coerce_dict('not-mapping')

    def test_count_records(self) -> None:
        """Test record counts differ for dicts vs. lists."""

        assert count_records({'a': 1}) == 1
        assert count_records([{'a': 1}, {'b': 2}]) == 2

    def test_to_float_bounds_and_default(self) -> None:
        """Test that :func:`to_float` respects defaults and bounds."""

        assert to_float('abc', default=1.5) == 1.5
        assert to_float('1', minimum=5) == 5
        assert to_float('10', maximum=3) == 3

    @pytest.mark.parametrize(
        'value,expected_result',
        [
            (2, 2.0),
            (2.5, 2.5),
            (' 2.5 ', 2.5),
            ('abc', None),
            (None, None),
        ],
    )
    def test_to_float_coercion(
        self,
        value: int | float | str | None,
        expected_result: float | None,
    ) -> None:
        """
        Test float coercion for various input types.

        Parameters
        ----------
        value : int | float | str | None
            Input value to coerce to float.
        expected_result : float | None
            Expected result after coercion.
        """
        assert to_float(value) == expected_result

    def test_float_helper_variants(self) -> None:
        """Test float helper variants for min/max/positivity handling."""

        assert to_maximum_float('1.5', default=2.0) == 2.0
        assert to_minimum_float('9.0', default=5.0) == 5.0
        assert to_positive_float('2.5') == 2.5
        assert to_positive_float('-1') is None

    def test_int_helper_variants(self) -> None:
        """Test integer helper variants for clamping logic."""

        assert to_maximum_int('7', default=10) == 10
        assert to_minimum_int('2', default=5) == 2
        assert to_positive_int('not-an-int', default=0, minimum=3) == 3

    def test_maybe_mapping(self) -> None:
        """Test mapping detection helper returns None for non-mappings."""

        mapping = {'x': 1}
        assert maybe_mapping(mapping) is mapping
        assert maybe_mapping(5) is None

    def test_normalize_str(self) -> None:
        """Test that whitespace and casing are stripped."""

        assert normalize_str('  HeLLo  ') == 'hello'
        assert normalize_str(None) == ''

    def test_print_json_uses_utf8(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Test that :func:`print_json` avoids ASCII escaping."""

        payload = {'emoji': '\u2603'}
        print_json(payload)
        captured = capsys.readouterr().out
        assert '\\u2603' not in captured
        assert 'emoji' in captured

    @pytest.mark.parametrize(
        'value,expected_result',
        [
            (10, 10),
            ('10', 10),
            ('  7  ', 7),
            ('3.0', 3),
            ('3.5', None),
            (None, None),
            ('abc', None),
        ],
    )
    def test_to_int_coercion(
        self,
        value: int | str | None,
        expected_result: int | None,
    ) -> None:
        """
        Test int coercion for various input types.

        Parameters
        ----------
        value : int | str | None
            Input value to coerce to int.
        expected_result : int | None
            Expected result after coercion.
        """
        assert to_int(value) == expected_result

    @pytest.mark.parametrize(
        'value',
        ['abc', '', '3.14.15'],
    )
    def test_to_number_with_invalid_strings(
        self,
        value: str,
    ) -> None:
        """
        Test :func:`to_number` with invalid string inputs.

        Parameters
        ----------
        value : str
            Input string to test.
        """
        assert to_number(value) is None

    @pytest.mark.parametrize(
        'value,expected_result',
        [
            ('42', 42.0),
            ('  10.5 ', 10.5),
        ],
    )
    def test_to_number_with_numeric_strings(
        self,
        value: str,
        expected_result: float,
    ) -> None:
        """
        Test :func:`to_number` with valid numeric string inputs.

        Parameters
        ----------
        value : str
            Input string to test.
        expected_result : float
            Expected result after conversion.
        """
        assert to_number(value) == expected_result

    @pytest.mark.parametrize(
        'value,expected_result',
        [
            (5, 5.0),
            (3.14, 3.14),
        ],
    )
    def test_to_number_with_numeric_types(
        self,
        value: int | float,
        expected_result: float,
    ) -> None:
        """
        Test :func:`to_number` with numeric types (int, float).

        Parameters
        ----------
        value : int | float
            Input value to test.
        expected_result : float
            Expected result after conversion.
        """
        assert to_number(value) == expected_result
