"""
tests.unit.test_u_utils module.

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


class TestUtils:
    """
    Unit test suite for ``etlplus.utils``.

    Notes
    -----
    - Unit tests for shared numeric coercion helpers.
    """

    @pytest.mark.parametrize(
        'value,expected',
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
        expected: float | None,
    ) -> None:
        """
        Test float coercion for various input types.

        Parameters
        ----------
        value : int | float | str | None
            Input value to coerce to float.
        expected : float | None
            Expected result after coercion.

        Returns
        -------
        None
        """
        assert to_float(value) == expected

    @pytest.mark.parametrize(
        'value,expected',
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
        expected: int | None,
    ) -> None:
        """
        Test int coercion for various input types.

        Parameters
        ----------
        value : int | str | None
            Input value to coerce to int.
        expected : int | None
            Expected result after coercion.

        Returns
        -------
        None
        """
        assert to_int(value) == expected

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

        Returns
        -------
        None
        """
        assert to_number(value) is None

    @pytest.mark.parametrize(
        'value,expected',
        [
            ('42', 42.0),
            ('  10.5 ', 10.5),
        ],
    )
    def test_to_number_with_numeric_strings(
        self,
        value: str,
        expected: float,
    ) -> None:
        """
        Test to_number with valid numeric string inputs.

        Parameters
        ----------
        value : str
            Input string to test.
        expected : float
            Expected result after conversion.

        Returns
        -------
        None
        """
        assert to_number(value) == expected

    @pytest.mark.parametrize(
        'value,expected',
        [
            (5, 5.0),
            (3.14, 3.14),
        ],
    )
    def test_to_number_with_numeric_types(
        self,
        value: int | float,
        expected: float,
    ) -> None:
        """
        Test to_number with numeric types (int, float).

        Parameters
        ----------
        value : int | float
            Input value to test.
        expected : float
            Expected result after conversion.

        Returns
        -------
        None
        """
        assert to_number(value) == expected
