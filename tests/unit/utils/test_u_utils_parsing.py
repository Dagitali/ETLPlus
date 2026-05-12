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
        ('value', 'default', 'expected'),
        [
            pytest.param(True, False, True, id='bool-true'),
            pytest.param(False, True, False, id='bool-false'),
            pytest.param(' yes ', False, True, id='true-string'),
            pytest.param('OFF', True, False, id='false-string'),
            pytest.param('maybe', True, True, id='unknown-string-default'),
            pytest.param(None, False, False, id='none-default'),
        ],
    )
    def test_bool_flag(
        self,
        value: object,
        default: bool,
        expected: bool,
    ) -> None:
        """Test common boolean flag parsing for config and env values."""
        assert ValueParser.bool_flag(value, default=default) is expected

    @pytest.mark.parametrize(
        ('value', 'expected'),
        [
            pytest.param(None, None, id='none'),
            pytest.param(' WARN ', 'warn', id='normalized-choice'),
            pytest.param('after_transform', 'after_transform', id='canonical-choice'),
            pytest.param('custom', 'custom', id='unknown-choice'),
            pytest.param(5, '5', id='coerced-unknown-choice'),
        ],
    )
    def test_optional_choice(
        self,
        value: object,
        expected: str | None,
    ) -> None:
        """Test optional choice normalization with string-preserving fallback."""
        choices = {
            'after_transform': 'after_transform',
            'warn': 'warn',
        }
        assert ValueParser.optional_choice(value, choices) == expected

    @pytest.mark.parametrize(
        ('value', 'expected'),
        [
            pytest.param(None, None, id='none'),
            pytest.param(5, 5, id='int'),
            pytest.param('5', 5, id='string-int'),
            pytest.param(5.0, 5, id='float-int'),
        ],
    )
    def test_optional_int(
        self,
        value: object,
        expected: int | None,
    ) -> None:
        """Test optional integer parsing for config-style values."""
        assert (
            ValueParser.optional_int(
                value,
                field_name='count',
                label='Example',
            )
            == expected
        )

    @pytest.mark.parametrize(
        'value',
        [
            pytest.param('not-an-int', id='string'),
            pytest.param(True, id='bool'),
            pytest.param(object(), id='object'),
        ],
    )
    def test_optional_int_rejects_invalid_values(self, value: object) -> None:
        """Test optional integer parsing rejects invalid values."""
        with pytest.raises(TypeError, match='Example "count" must be an integer'):
            ValueParser.optional_int(value, field_name='count', label='Example')

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
        'data',
        [
            pytest.param({}, id='missing'),
            pytest.param({'name': 1}, id='non-string'),
        ],
    )
    def test_require_str_raises_for_missing_or_invalid_field(
        self,
        data: Mapping[str, object],
    ) -> None:
        """Test strict required string extraction error messages."""
        with pytest.raises(TypeError, match='ConnectorFile requires a "name"'):
            MappingFieldParser.require_str(data, 'name', label='ConnectorFile')

    def test_require_str_returns_valid_field(self) -> None:
        """Test strict required string extraction for valid payloads."""
        assert (
            MappingFieldParser.require_str(
                {'name': 'etl'},
                'name',
                label='ConnectorFile',
            )
            == 'etl'
        )

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
            pytest.param(['a'], True, id='list'),
            pytest.param(('a',), True, id='tuple'),
            pytest.param('abc', False, id='string'),
            pytest.param(b'abc', False, id='bytes'),
            pytest.param(bytearray(b'abc'), False, id='bytearray'),
            pytest.param({'a', 'b'}, False, id='set'),
            pytest.param(None, False, id='none'),
        ],
    )
    def test_is_non_text(
        self,
        value: object,
        expected: bool,
    ) -> None:
        """Test that non-text sequence detection excludes text and sets."""
        assert SequenceParser.is_non_text(value) is expected

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
