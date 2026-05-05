"""
:mod:`tests.unit.utils.test_u_utils_data` module.

Unit tests for :mod:`etlplus.utils._data`.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import date
from datetime import datetime
from datetime import time
from decimal import Decimal
from io import StringIO

import pytest

from etlplus.utils import JsonCodec
from etlplus.utils import RecordCounter
from etlplus.utils._types import JSONData

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


class _FalseyStringIO(StringIO):
    """String buffer that intentionally evaluates false."""

    def __bool__(self) -> bool:
        return False


# SECTION: TESTS ============================================================ #


class TestDataHelpers:
    """Unit tests for general data helpers."""

    @pytest.mark.parametrize(
        ('payload', 'expected'),
        [
            pytest.param({'id': 1}, 1, id='single-dict'),
            pytest.param({}, 1, id='empty-dict-still-single-record'),
            pytest.param([{'id': 1}], 1, id='single-item-list'),
            pytest.param([{'id': 1}, {'id': 2}], 2, id='two-item-list'),
            pytest.param([], 0, id='empty-list'),
        ],
    )
    def test_count_records(
        self,
        payload: JSONData,
        expected: int,
    ) -> None:
        """
        Test that :meth:`RecordCounter.count` counts dict and list payloads
        consistently.
        """
        assert RecordCounter.count(payload) == expected

    @pytest.mark.parametrize(
        ('value', 'expected'),
        [
            pytest.param(date(2026, 1, 2), '"2026-01-02"', id='date'),
            pytest.param(Decimal('1.20'), '"1.20"', id='decimal'),
            pytest.param(object(), '"<object object at ', id='fallback-prefix'),
        ],
    )
    def test_default_supports_common_non_json_values(
        self,
        value: object,
        expected: str,
    ) -> None:
        """Test the shared fallback serializer used by database codecs."""
        serialized = JsonCodec.serialize(value, default=JsonCodec.default)

        assert serialized.startswith(expected)

    @pytest.mark.parametrize(
        ('value', 'expected'),
        [
            pytest.param(date(2026, 1, 2), '2026-01-02', id='date'),
            pytest.param(
                datetime(2026, 1, 2, 3, 4, 5, 6),
                '2026-01-02T03:04:05.000006',
                id='datetime',
            ),
            pytest.param(time(3, 4, 5, 6), '03:04:05.000006', id='time'),
        ],
    )
    def test_isoformat_uses_stable_precision(
        self,
        value: date | datetime | time,
        expected: str,
    ) -> None:
        """Test that date-like values render with stable ISO precision."""
        assert JsonCodec.isoformat(value) == expected

    def test_parse_json_raises_concise_value_error(self) -> None:
        """Test that :meth:`JsonCodec.parse` wraps JSON decode failures cleanly."""
        with pytest.raises(ValueError, match=r'^Invalid JSON payload:'):
            JsonCodec.parse('{bad json}')

    @pytest.mark.parametrize(
        'text',
        [
            pytest.param('"scalar"', id='string'),
            pytest.param('1', id='number'),
            pytest.param('true', id='boolean'),
            pytest.param('null', id='null'),
        ],
    )
    def test_parse_json_rejects_scalar_payloads(self, text: str) -> None:
        """Test that parsed ETL payloads must be JSON objects or arrays."""
        with pytest.raises(ValueError, match='object or array'):
            JsonCodec.parse(text)

    def test_parse_json_returns_json_payload(self) -> None:
        """Test that :meth:`JsonCodec.parse` returns decoded JSON data."""
        assert JsonCodec.parse('{"name": "Ada"}') == {'name': 'Ada'}

    def test_print_json_uses_utf8_without_ascii_escaping(
        self,
        unicode_payload: dict[str, str],
        capsys: pytest.CaptureFixture[str],
        assert_json_output: Callable[[str, object], None],
    ) -> None:
        """
        Test that :meth:`JsonCodec.print` preserves readable Unicode output.
        """
        JsonCodec.print(unicode_payload)
        captured = capsys.readouterr().out

        assert '\\u2603' not in captured
        assert_json_output(captured, unicode_payload)

    def test_print_json_honors_falsey_stream(
        self,
        assert_json_output: Callable[[str, object], None],
    ) -> None:
        """Test that explicit streams are used even when they evaluate false."""
        stream = _FalseyStringIO()
        payload = {'ok': True}

        JsonCodec.print(payload, stream=stream)

        assert_json_output(stream.getvalue(), payload)

    def test_serialize_json_can_use_standard_spacing(self) -> None:
        """Test that compact output can be disabled for standard JSON spacing."""
        assert JsonCodec.serialize({'a': 1}, compact=False) == '{"a": 1}'

    def test_serialize_json_compacts_by_default(self) -> None:
        """Test that :meth:`JsonCodec.serialize` emits compact JSON by default."""
        assert JsonCodec.serialize({'b': 1, 'a': 2}, sort_keys=True) == '{"a":2,"b":1}'

    def test_serialize_json_pretty_prints_when_requested(self) -> None:
        """Test that :meth:`JsonCodec.serialize` supports stable pretty output."""
        assert JsonCodec.serialize({'a': 1}, pretty=True) == '{\n  "a": 1\n}'
