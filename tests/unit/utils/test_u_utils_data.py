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
from etlplus.utils import RecordPayloadParser
from etlplus.utils import coerce_record_payload
from etlplus.utils import normalize_records
from etlplus.utils import stringify_value
from etlplus.utils._types import JSONData

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


class _FalseyStringIO(StringIO):
    """String buffer that intentionally evaluates false."""

    def __bool__(self) -> bool:
        return False


# SECTION: FIXTURES ========================================================= #


@pytest.fixture(name='json_record_parser')
def json_record_parser_fixture() -> RecordPayloadParser:
    """Return one parser with stable JSON error-message context."""
    return RecordPayloadParser('JSON')


# SECTION: TESTS ============================================================ #


class TestDataHelpers:
    """Unit tests for general data helpers."""

    @pytest.mark.parametrize(
        ('payload', 'expected'),
        [
            pytest.param({'a': 1}, {'a': 1}, id='dict'),
            pytest.param([{'a': 1}, {'a': 2}], [{'a': 1}, {'a': 2}], id='records'),
        ],
    )
    def test_coerce_record_payload_accepts_valid_shapes(
        self,
        json_record_parser: RecordPayloadParser,
        payload: object,
        expected: JSONData,
    ) -> None:
        """Test record payload coercion for supported and invalid shapes."""
        assert json_record_parser.coerce(payload) == expected

    @pytest.mark.parametrize(
        ('payload', 'pattern'),
        [
            pytest.param([{'a': 1}, 2], 'array must contain only objects', id='mixed'),
            pytest.param('bad', 'root must be an object', id='scalar'),
        ],
    )
    def test_coerce_record_payload_rejects_invalid_shapes(
        self,
        json_record_parser: RecordPayloadParser,
        payload: object,
        pattern: str,
    ) -> None:
        """Test record payload coercion rejects invalid shapes."""
        with pytest.raises(TypeError, match=pattern):
            json_record_parser.coerce(payload)

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
        serialized = JsonCodec(default_serializer=JsonCodec.default).serialize(value)

        assert serialized.startswith(expected)

    def test_decode_json_returns_raw_json_value(self) -> None:
        """Test that raw JSON decoding does not enforce ETL payload shape."""
        assert JsonCodec.decode('42') == 42

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

    def test_json_codec_is_frozen(self) -> None:
        """Test that render policy cannot be mutated after construction."""
        codec = JsonCodec()

        with pytest.raises(AttributeError):
            codec.pretty = True  # type: ignore[misc]

    @pytest.mark.parametrize(
        ('payload', 'expected'),
        [
            pytest.param({'id': 1}, [{'id': 1}], id='dict'),
            pytest.param([{'id': 1}, {'id': 2}], [{'id': 1}, {'id': 2}], id='records'),
        ],
    )
    def test_normalize_records_accepts_valid_shapes(
        self,
        json_record_parser: RecordPayloadParser,
        payload: object,
        expected: list[dict[str, object]],
    ) -> None:
        """Test record normalization for valid and invalid payloads."""
        assert json_record_parser.normalize(payload) == expected

    @pytest.mark.parametrize(
        ('payload', 'pattern'),
        [
            pytest.param(1, 'must be an object or an array', id='scalar'),
            pytest.param([{'id': 1}, 'x'], 'contain only objects', id='mixed'),
        ],
    )
    def test_normalize_records_rejects_invalid_shapes(
        self,
        json_record_parser: RecordPayloadParser,
        payload: object,
        pattern: str,
    ) -> None:
        """Test record normalization rejects invalid shapes."""
        with pytest.raises(TypeError, match=pattern):
            json_record_parser.normalize(payload)

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
            pytest.param('[1, 2]', id='scalar-array'),
            pytest.param('[{"id": 1}, 2]', id='mixed-array'),
        ],
    )
    def test_parse_json_rejects_scalar_payloads(self, text: str) -> None:
        """Test that parsed ETL payloads must be JSON objects or arrays."""
        with pytest.raises(ValueError, match='object or array of objects'):
            JsonCodec.parse(text)

    @pytest.mark.parametrize(
        ('text', 'expected'),
        [
            pytest.param('{"name": "Ada"}', {'name': 'Ada'}, id='object'),
            pytest.param('[{"id": 1}]', [{'id': 1}], id='record-array'),
            pytest.param('[]', [], id='empty-array'),
        ],
    )
    def test_parse_json_returns_json_payload(
        self,
        text: str,
        expected: JSONData,
    ) -> None:
        """Test that :meth:`JsonCodec.parse` returns decoded JSON data."""
        assert JsonCodec.parse(text) == expected

    def test_print_json_uses_utf8_without_ascii_escaping(
        self,
        unicode_payload: dict[str, str],
        capsys: pytest.CaptureFixture[str],
        assert_json_output: Callable[[str, object], None],
    ) -> None:
        """
        Test that :meth:`JsonCodec.print` preserves readable Unicode output.
        """
        JsonCodec(pretty=True).print(unicode_payload)
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

        JsonCodec(pretty=True).print(payload, stream=stream)

        assert_json_output(stream.getvalue(), payload)

    def test_record_payload_parser_is_frozen(self) -> None:
        """Test that parser context cannot be mutated after construction."""
        parser = RecordPayloadParser('JSON')

        with pytest.raises(AttributeError):
            parser.format_name = 'CSV'  # type: ignore[misc]

    def test_record_payload_parser_wrappers_preserve_function_api(self) -> None:
        """Test compatibility wrappers delegate to the stateful parser."""
        assert coerce_record_payload({'id': 1}, format_name='JSON') == {'id': 1}
        assert normalize_records({'id': 1}, 'JSON') == [{'id': 1}]

    def test_require_dict_payload_and_require_str_key(
        self,
        json_record_parser: RecordPayloadParser,
    ) -> None:
        """Test dict/string key payload validators."""
        payload = json_record_parser.require_dict({'key': 'value'})
        assert payload == {'key': 'value'}
        assert json_record_parser.require_str_key(payload, 'key') == 'value'
        with pytest.raises(TypeError, match='must be a dict'):
            json_record_parser.require_dict([])
        with pytest.raises(TypeError, match='must include a "key" string'):
            json_record_parser.require_str_key({'key': 1}, 'key')

    def test_serialize_json_can_use_standard_spacing(self) -> None:
        """Test that compact output can be disabled for standard JSON spacing."""
        assert JsonCodec(compact=False).serialize({'a': 1}) == '{"a": 1}'

    def test_serialize_json_compacts_by_default(self) -> None:
        """Test that :meth:`JsonCodec.serialize` emits compact JSON by default."""
        assert JsonCodec(sort_keys=True).serialize({'b': 1, 'a': 2}) == '{"a":2,"b":1}'

    def test_serialize_json_pretty_prints_when_requested(self) -> None:
        """Test that :meth:`JsonCodec.serialize` supports stable pretty output."""
        assert JsonCodec(pretty=True).serialize({'a': 1}) == '{\n  "a": 1\n}'

    @pytest.mark.parametrize(
        ('value', 'expected'),
        [
            pytest.param(None, '', id='none'),
            pytest.param(12, '12', id='integer'),
            pytest.param('abc', 'abc', id='string'),
        ],
    )
    def test_stringify_value(
        self,
        value: object,
        expected: str,
    ) -> None:
        """Test scalar stringification rules."""
        assert stringify_value(value) == expected
