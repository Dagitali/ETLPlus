"""
:mod:`tests.unit.utils.test_u_utils_numbers` module.

Unit tests for :mod:`etlplus.utils._numbers`.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest

from etlplus.utils import to_float
from etlplus.utils import to_int
from etlplus.utils import to_maximum_float
from etlplus.utils import to_maximum_int
from etlplus.utils import to_minimum_float
from etlplus.utils import to_minimum_int
from etlplus.utils import to_number
from etlplus.utils import to_positive_float
from etlplus.utils import to_positive_int

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
    def test_to_float_coercion(
        self,
        value: object,
        expected: float | None,
    ) -> None:
        """
        Test that :func:`to_float` coerces valid values and rejects invalid
        ones.
        """
        assert to_float(value) == expected

    @pytest.mark.parametrize(
        ('value', 'kwargs', 'expected'),
        [
            pytest.param('abc', {'default': 1.5}, 1.5, id='default-on-failure'),
            pytest.param('1', {'minimum': 5}, 5, id='minimum-clamp'),
            pytest.param('10', {'maximum': 3}, 3, id='maximum-clamp'),
        ],
    )
    def test_to_float_bounds_and_default(
        self,
        value: object,
        kwargs: dict[str, float],
        expected: float,
    ) -> None:
        """
        Test that :func:`to_float` applies defaults and clamp bounds
        consistently.
        """
        assert to_float(value, **kwargs) == expected

    @pytest.mark.parametrize(
        ('func', 'args', 'expected'),
        [
            pytest.param(to_maximum_float, ('1.5', 2.0), 2.0, id='to_maximum_float'),
            pytest.param(to_minimum_float, ('9.0', 5.0), 5.0, id='to_minimum_float'),
            pytest.param(
                to_positive_float,
                ('2.5',),
                2.5,
                id='to_positive_float-positive',
            ),
            pytest.param(
                to_positive_float,
                ('-1',),
                None,
                id='to_positive_float-non-positive',
            ),
        ],
    )
    def test_float_helper_variants(
        self,
        func: Callable[..., float | None],
        args: tuple[object, ...],
        expected: float | None,
    ) -> None:
        """
        Test that float helper wrappers delegate to the shared coercion rules.
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
    def test_to_int_coercion(
        self,
        value: object,
        expected: int | None,
    ) -> None:
        """
        Test that :func:`to_int` coerces valid integer inputs and rejects
        invalid ones.
        """
        assert to_int(value) == expected

    def test_to_int_raises_on_invalid_bounds(self) -> None:
        """
        Test that :func:`to_int` rejects invalid minimum and maximum bounds.
        """
        with pytest.raises(ValueError, match='minimum cannot exceed maximum'):
            to_int(5, minimum=10, maximum=1)

    @pytest.mark.parametrize(
        ('func', 'args', 'expected'),
        [
            pytest.param(to_maximum_int, ('7', 10), 10, id='to_maximum_int'),
            pytest.param(to_minimum_int, ('2', 5), 2, id='to_minimum_int'),
        ],
    )
    def test_int_bound_helper_variants(
        self,
        func: Callable[..., int],
        args: tuple[object, ...],
        expected: int,
    ) -> None:
        """
        Test that min/max int wrappers preserve their helper semantics.
        """
        assert func(*args) == expected

    def test_to_positive_int_minimum_fallback(self) -> None:
        """
        Test that :func:`to_positive_int` falls back to the provided positive
        minimum.
        """
        assert to_positive_int('not-an-int', default=0, minimum=3) == 3


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
    def test_to_number(
        self,
        value: object,
        expected: float | None,
    ) -> None:
        """
        Test that :func:`to_number` follows float-style coercion while rejecting bools.
        """
        assert to_number(value) == expected

    def test_to_number_with_shared_non_mapping_cases(
        self,
        non_mapping_value: object,
    ) -> None:
        """
        Test that shared non-mapping fixture values remain safe for coercion.
        """
        assert to_number(non_mapping_value) is None
