"""
:mod:`tests.unit.utils.test_u_utils_parsing` module.

Unit tests for :mod:`etlplus.utils._parsing`.
"""

from __future__ import annotations

from collections.abc import Mapping

import pytest

from etlplus.utils._parsing import MappingFieldParser
from etlplus.utils._parsing import SequenceParser
from etlplus.utils._parsing import ValueParser

# SECTION: TESTS ============================================================ #


class TestValueParser:
    """Unit tests for scalar parsing helpers."""

    @pytest.mark.parametrize(
        ('value', 'expected'),
        [
            pytest.param(None, None, id='none'),
            pytest.param('hello', 'hello', id='string'),
            pytest.param(5, '5', id='coerce-int'),
            pytest.param(False, 'False', id='coerce-bool'),
        ],
    )
    def test_optional_str(
        self,
        value: object,
        expected: str | None,
    ) -> None:
        """Test that optional string parsing preserves or coerces values."""
        assert ValueParser.optional_str(value) == expected


class TestMappingFieldParser:
    """Unit tests for mapping field extraction helpers."""

    @pytest.mark.parametrize(
        ('data', 'key', 'expected'),
        [
            pytest.param({'name': 'etl'}, 'name', 'etl', id='string-present'),
            pytest.param({'name': 1}, 'name', None, id='non-string'),
            pytest.param({}, 'name', None, id='missing'),
        ],
    )
    def test_required_str(
        self,
        data: Mapping[str, object],
        key: str,
        expected: str | None,
    ) -> None:
        """Test that required string extraction rejects non-string values."""
        assert MappingFieldParser.required_str(data, key) == expected


class TestSequenceParser:
    """Unit tests for sequence parsing helpers."""

    @pytest.mark.parametrize(
        ('value', 'expected'),
        [
            pytest.param('seed', ['seed'], id='single-string'),
            pytest.param(['a', 1, None, 'b'], ['a', 'b'], id='filter-non-strings'),
            pytest.param(('a', 'b'), ['a', 'b'], id='tuple'),
            pytest.param({'a', 'b'}, [], id='set-is-not-sequence'),
            pytest.param(None, [], id='none'),
        ],
    )
    def test_str_list(
        self,
        value: object,
        expected: list[str],
    ) -> None:
        """Test that string-list parsing preserves only string entries."""
        assert SequenceParser.str_list(value) == expected
