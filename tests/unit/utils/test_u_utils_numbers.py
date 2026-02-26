"""
:mod:`tests.unit.utils.test_u_utils_numbers` module.

Unit tests for :mod:`etlplus.utils.numbers`.
"""

from __future__ import annotations

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

# SECTION: TESTS ============================================================ #


class TestNumbers:
    """Unit tests for :mod:`etlplus.utils.numbers`."""

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

    def test_to_int_additional_branches(self) -> None:
        """Test remaining int coercion and bounds validation branches."""
        assert to_int(3.0) == 3
        assert to_int('   ') is None
        with pytest.raises(ValueError, match='minimum cannot exceed maximum'):
            to_int(5, minimum=10, maximum=1)

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
