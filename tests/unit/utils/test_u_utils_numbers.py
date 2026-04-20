"""
:mod:`tests.unit.utils.test_u_utils_numbers` module.

Unit tests for :mod:`etlplus.utils._numbers`.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest

from etlplus.utils import FloatParser
from etlplus.utils import IntParser

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

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

    def test_int_parser_parse_raises_on_invalid_bounds(self) -> None:
        """
        Test that :meth:`IntParser.parse` rejects invalid minimum and maximum bounds.
        """
        with pytest.raises(ValueError, match='minimum cannot exceed maximum'):
            IntParser.parse(5, minimum=10, maximum=1)

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
            pytest.param('42', 42.0, id='int-string'),
            pytest.param('  10.5 ', 10.5, id='float-string'),
            pytest.param(5, 5.0, id='int'),
            pytest.param(3.14, 3.14, id='float'),
            pytest.param('abc', None, id='invalid-string'),
            pytest.param('', None, id='blank-string'),
            pytest.param('3.14.15', None, id='malformed-number'),
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

    def test_float_parser_coerce_with_shared_non_mapping_cases(
        self,
        non_mapping_value: object,
    ) -> None:
        """
        Test that shared non-mapping fixture values remain safe for coercion.
        """
        assert FloatParser.coerce(non_mapping_value) is None
