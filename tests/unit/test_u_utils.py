"""
tests.unit.test_u_utils module.

Unit tests for ``etlplus.utils``.

Notes
-----
- Unit tests for shared numeric coercion helpers.
"""
from __future__ import annotations

from etlplus.utils import to_float
from etlplus.utils import to_int
from etlplus.utils import to_number


# SECTION: TESTS =========================================================== #


def test_to_float_coercion():
    assert to_float(2) == 2.0
    assert to_float(2.5) == 2.5
    assert to_float(' 2.5 ') == 2.5
    assert to_float('abc') is None
    assert to_float(None) is None


def test_to_int_coercion():
    assert to_int(10) == 10
    assert to_int('10') == 10
    assert to_int('  7  ') == 7

    # Decimal numeric string coerces only if exact integer.
    assert to_int('3.0') == 3
    assert to_int('3.5') is None

    assert to_int(None) is None
    assert to_int('abc') is None


def test_to_number_with_invalid_strings():
    assert to_number('abc') is None
    assert to_number('') is None
    assert to_number('3.14.15') is None


def test_to_number_with_numeric_strings():
    assert to_number('42') == 42.0
    assert to_number('  10.5 ') == 10.5


def test_to_number_with_numeric_types():
    assert to_number(5) == 5.0
    assert to_number(3.14) == 3.14
