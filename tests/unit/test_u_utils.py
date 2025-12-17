"""
:mod:`tests.unit.test_u_utils` module.

Unit tests for ``etlplus.utils``.

Notes
-----
- Unit tests for shared numeric coercion helpers.
"""

from __future__ import annotations

import pytest

from etlplus.utils import to_float
from etlplus.utils import to_int
from etlplus.utils import to_number

# SECTION: TESTS =========================================================== #


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
        Test to_number with invalid string inputs.

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
        Test to_number with valid numeric string inputs.

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
        Test to_number with numeric types (int, float).

        Parameters
        ----------
        value : int | float
            Input value to test.
        expected_result : float
            Expected result after conversion.
        """
        assert to_number(value) == expected_result
