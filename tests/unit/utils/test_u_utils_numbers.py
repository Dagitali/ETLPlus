"""
:mod:`tests.unit.utils.test_u_utils_numbers` module.

Unit tests for :mod:`etlplus.utils._numbers`.
"""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal

import pytest

from etlplus.utils import FloatParser
from etlplus.utils import IntParser
from etlplus.utils import finite_decimal_or_none
from etlplus.utils import is_integer_value
from etlplus.utils import is_number_value

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


@pytest.fixture(name='floatable_value')
def floatable_value_fixture() -> object:
    """Return one object exposing ``__float__`` for coercion tests."""

    class Floatable:
        """Object exposing ``__float__`` for parser fallback tests."""

        def __float__(self) -> float:
            """Return a finite float value."""
            return 2.5

    return Floatable()


# SECTION: TESTS ============================================================ #


class TestFloatCoercion:
    """Unit tests for float-oriented coercion helpers."""

    @pytest.mark.parametrize(
        ('value', 'expected'),
        [
            pytest.param(2, 2.0, id='int'),
            pytest.param(2.5, 2.5, id='float'),
            pytest.param(' 2.5 ', 2.5, id='numeric-string'),
            pytest.param('abc', None, id='invalid-string'),
            pytest.param(None, None, id='none'),
            pytest.param(True, None, id='bool-rejected'),
        ],
    )
    def test_float_parser_parse_coercion(
        self,
        value: object,
        expected: float | None,
    ) -> None:
        """
        Test that :meth:`FloatParser.parse` coerces valid values and rejects
        invalid ones.
        """
        assert FloatParser.parse(value) == expected

    @pytest.mark.parametrize(
        ('value', 'kwargs', 'expected'),
        [
            pytest.param('abc', {'default': 1.5}, 1.5, id='default-on-failure'),
            pytest.param('1', {'minimum': 5}, 5, id='minimum-clamp'),
            pytest.param('10', {'maximum': 3}, 3, id='maximum-clamp'),
        ],
    )
    def test_float_parser_parse_bounds_and_default(
        self,
        value: object,
        kwargs: dict[str, float],
        expected: float,
    ) -> None:
        """
        Test that :meth:`FloatParser.parse` applies defaults and clamp bounds
        consistently.
        """
        assert FloatParser.parse(value, **kwargs) == expected

    @pytest.mark.parametrize(
        ('func', 'args', 'expected'),
        [
            pytest.param(FloatParser.at_least, ('1.5', 2.0), 2.0, id='at_least'),
            pytest.param(FloatParser.at_most, ('9.0', 5.0), 5.0, id='at_most'),
            pytest.param(FloatParser.positive, ('2.5',), 2.5, id='positive'),
            pytest.param(FloatParser.positive, ('-1',), None, id='non-positive'),
        ],
    )
    def test_float_parser_variants(
        self,
        func: Callable[..., float | None],
        args: tuple[object, ...],
        expected: float | None,
    ) -> None:
        """
        Test that float parser variants delegate to the shared coercion rules.
        """
        assert func(*args) == expected


class TestIntCoercion:
    """Unit tests for integer-oriented coercion helpers."""

    @pytest.mark.parametrize(
        ('func', 'args', 'expected'),
        [
            pytest.param(IntParser.at_least, ('7', 10), 10, id='at_least'),
            pytest.param(IntParser.at_most, ('2', 5), 2, id='at_most'),
        ],
    )
    def test_int_parser_bound_variants(
        self,
        func: Callable[..., int],
        args: tuple[object, ...],
        expected: int,
    ) -> None:
        """
        Test that parser bound helpers preserve their numeric semantics.
        """
        assert func(*args) == expected

    @pytest.mark.parametrize(
        ('value', 'expected'),
        [
            pytest.param(10, 10, id='int'),
            pytest.param(3.0, 3, id='int-float'),
            pytest.param(3.5, None, id='non-int-float'),
            pytest.param('10', 10, id='int-string'),
            pytest.param('  7  ', 7, id='trimmed-int-string'),
            pytest.param('   ', None, id='blank-trimmed-string'),
            pytest.param('3.0', 3, id='int-float-string'),
            pytest.param('3.5', None, id='non-int-float-string'),
            pytest.param(None, None, id='none'),
            pytest.param('abc', None, id='invalid-string'),
            pytest.param(False, None, id='bool-rejected'),
        ],
    )
    def test_int_parser_parse_coercion(
        self,
        value: object,
        expected: int | None,
    ) -> None:
        """
        Test that :meth:`IntParser.parse` coerces valid integer inputs and rejects
        invalid ones.
        """
        assert IntParser.parse(value) == expected

    @pytest.mark.parametrize(
        ('value', 'default'),
        [
            pytest.param(5, None, id='parsed-value'),
            pytest.param('not-an-int', None, id='unparsed-no-default'),
            pytest.param('not-an-int', 5, id='unparsed-with-default'),
        ],
    )
    def test_int_parser_parse_raises_on_invalid_bounds(
        self,
        value: object,
        default: int | None,
    ) -> None:
        """
        Test that :meth:`IntParser.parse` always rejects invalid bounds.
        """
        with pytest.raises(ValueError, match='minimum cannot exceed maximum'):
            IntParser.parse(value, default=default, minimum=10, maximum=1)

    def test_int_parser_positive_minimum_fallback(self) -> None:
        """
        Test that :meth:`IntParser.positive` falls back to the provided
        positive
        minimum.
        """
        assert IntParser.positive('not-an-int', default=0, minimum=3) == 3


class TestGenericNumberCoercion:
    """Unit tests for generic number coercion."""

    @pytest.mark.parametrize(
        ('value', 'expected'),
        [
            pytest.param(1, Decimal('1'), id='int'),
            pytest.param(1.5, Decimal('1.5'), id='float'),
            pytest.param(Decimal('1.20'), Decimal('1.20'), id='decimal'),
            pytest.param(' 1e3 ', Decimal('1E+3'), id='scientific-string'),
            pytest.param(float('nan'), None, id='nan'),
            pytest.param(float('inf'), None, id='inf'),
            pytest.param('bad', None, id='invalid-string'),
            pytest.param(object(), None, id='object'),
        ],
    )
    def test_finite_decimal_or_none(
        self,
        value: object,
        expected: Decimal | None,
    ) -> None:
        """Test finite decimal coercion for numeric-like values."""
        assert finite_decimal_or_none(value) == expected

    @pytest.mark.parametrize(
        ('value', 'expected'),
        [
            pytest.param('42', 42.0, id='int-string'),
            pytest.param('  10.5 ', 10.5, id='float-string'),
            pytest.param(5, 5.0, id='int'),
            pytest.param(3.14, 3.14, id='float'),
            pytest.param('abc', None, id='invalid-string'),
            pytest.param('', None, id='blank-string'),
            pytest.param('3.14.15', None, id='malformed-number'),
            pytest.param(float('nan'), None, id='nan'),
            pytest.param(float('inf'), None, id='inf'),
            pytest.param('-inf', None, id='negative-inf-string'),
            pytest.param(True, None, id='bool-rejected'),
        ],
    )
    def test_float_parser_coerce(
        self,
        value: object,
        expected: float | None,
    ) -> None:
        """
        Test that :meth:`FloatParser.coerce` follows float-style coercion
        while rejecting bools.
        """
        assert FloatParser.coerce(value) == expected

    def test_float_parser_coerce_accepts_floatable_object(
        self,
        floatable_value: object,
    ) -> None:
        """Test fallback float coercion for non-primitive floatable objects."""
        assert FloatParser.coerce(floatable_value) == 2.5

    def test_float_parser_coerce_with_shared_non_mapping_cases(
        self,
        non_mapping_value: object,
    ) -> None:
        """
        Test that shared non-mapping fixture values remain safe for coercion.
        """
        assert FloatParser.coerce(non_mapping_value) is None

    @pytest.mark.parametrize(
        ('value', 'expected'),
        [
            pytest.param(1, True, id='int'),
            pytest.param(False, False, id='bool'),
            pytest.param(1.0, False, id='float'),
            pytest.param('1', False, id='string'),
        ],
    )
    def test_is_integer_value(
        self,
        value: object,
        expected: bool,
    ) -> None:
        """Test JSON-style integer predicate semantics."""
        assert is_integer_value(value) is expected

    @pytest.mark.parametrize(
        ('value', 'expected'),
        [
            pytest.param(1, True, id='int'),
            pytest.param(1.5, True, id='float'),
            pytest.param(True, False, id='bool'),
            pytest.param(Decimal('1.0'), False, id='decimal'),
            pytest.param('1', False, id='string'),
        ],
    )
    def test_is_number_value(
        self,
        value: object,
        expected: bool,
    ) -> None:
        """Test JSON-style number predicate semantics."""
        assert is_number_value(value) is expected
