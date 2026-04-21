"""
:mod:`tests.unit.utils.test_u_utils_data` module.

Unit tests for :mod:`etlplus.utils._data`.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest

from etlplus.utils import JsonCodec
from etlplus.utils import RecordCounter
from etlplus.utils._types import JSONData

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

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

    def test_parse_json_raises_concise_value_error(self) -> None:
        """Test that :meth:`JsonCodec.parse` wraps JSON decode failures cleanly."""
        with pytest.raises(ValueError, match=r'^Invalid JSON payload:'):
            JsonCodec.parse('{bad json}')

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

    def test_serialize_json_compacts_by_default(self) -> None:
        """Test that :meth:`JsonCodec.serialize` emits compact JSON by default."""
        assert JsonCodec.serialize({'b': 1, 'a': 2}, sort_keys=True) == '{"a":2,"b":1}'

    def test_serialize_json_pretty_prints_when_requested(self) -> None:
        """Test that :meth:`JsonCodec.serialize` supports stable pretty output."""
        assert JsonCodec.serialize({'a': 1}, pretty=True) == '{\n  "a": 1\n}'
