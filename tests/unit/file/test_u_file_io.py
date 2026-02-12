"""
:mod:`tests.unit.file.test_u_file_io` module.

Unit tests for :mod:`etlplus.file._io`.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from etlplus.file import _io as mod

# SECTION: INTERNAL CLASSES ================================================= #


class _TableStub:
    """Minimal table-like stub with ``to_dict`` API."""

    def __init__(
        self,
        records: list[dict[str, object]],
    ) -> None:
        self._records = records

    def to_dict(
        self,
        *,
        orient: str,
    ) -> list[dict[str, object]]:
        """Return records for the expected orient."""
        assert orient == 'records'
        return list(self._records)


# SECTION: TESTS ============================================================ #


class TestIoHelpers:
    """Unit tests for shared file IO helpers."""

    def test_coerce_path_accepts_str_and_path(self, tmp_path: Path) -> None:
        """Test path coercion from strings and existing Path objects."""
        value = tmp_path / 'file.txt'
        assert mod.coerce_path(str(value)) == value
        assert mod.coerce_path(value) == value

    def test_coerce_record_payload_accepts_object_or_object_array(
        self,
    ) -> None:
        """Test record payload coercion for supported shapes."""
        assert mod.coerce_record_payload(
            {'a': 1},
            format_name='JSON',
        ) == {'a': 1}
        assert mod.coerce_record_payload(
            [{'a': 1}, {'a': 2}],
            format_name='JSON',
        ) == [{'a': 1}, {'a': 2}]

    def test_coerce_record_payload_rejects_invalid_shapes(self) -> None:
        """Test record payload coercion failures for invalid shapes."""
        with pytest.raises(TypeError, match='array must contain only objects'):
            mod.coerce_record_payload([{'a': 1}, 2], format_name='JSON')
        with pytest.raises(TypeError, match='root must be an object'):
            mod.coerce_record_payload('bad', format_name='JSON')

    def test_normalize_records_accepts_dict_and_list(self) -> None:
        """Test record normalization for dict and list payloads."""
        assert mod.normalize_records({'id': 1}, 'CSV') == [{'id': 1}]
        records = [{'id': 1}, {'id': 2}]
        assert mod.normalize_records(records, 'CSV') == records

    def test_normalize_records_rejects_non_object_payloads(self) -> None:
        """Test record normalization failures."""
        with pytest.raises(TypeError, match='must be an object or an array'):
            mod.normalize_records(1, 'CSV')  # type: ignore[arg-type]
        with pytest.raises(TypeError, match='contain only objects'):
            invalid_rows: Any = [{'id': 1}, 'x']
            mod.normalize_records(
                invalid_rows,
                'CSV',
            )

    def test_require_dict_payload_and_require_str_key(self) -> None:
        """Test dict/string key payload validators."""
        payload = mod.require_dict_payload({'key': 'value'}, format_name='INI')
        assert payload == {'key': 'value'}
        assert (
            mod.require_str_key(payload, format_name='INI', key='key')
            == 'value'
        )
        with pytest.raises(TypeError, match='must be a dict'):
            mod.require_dict_payload(
                [],
                format_name='INI',
            )  # type: ignore[arg-type]
        with pytest.raises(TypeError, match='must include a "key" string'):
            mod.require_str_key({'key': 1}, format_name='INI', key='key')

    def test_stringify_value(self) -> None:
        """Test scalar stringification rules."""
        assert mod.stringify_value(None) == ''
        assert mod.stringify_value(12) == '12'
        assert mod.stringify_value('abc') == 'abc'

    def test_records_from_table(self) -> None:
        """Test conversion from dataframe-like objects."""
        table = _TableStub([{'id': 1}, {'id': 2}])
        assert mod.records_from_table(table) == [{'id': 1}, {'id': 2}]

    def test_read_and_write_text(
        self,
        tmp_path: Path,
    ) -> None:
        """Test text writing/reading, including trailing newline behavior."""
        file_path = tmp_path / 'out' / 'data.txt'
        mod.write_text(file_path, 'line', trailing_newline=True)
        assert file_path.read_text(encoding='utf-8') == 'line\n'
        assert mod.read_text(file_path) == 'line\n'

    def test_read_and_write_delimited(
        self,
        tmp_path: Path,
    ) -> None:
        """Test delimited writer/reader round trip."""
        file_path = tmp_path / 'out' / 'rows.csv'
        count = mod.write_delimited(
            file_path,
            [{'b': 2, 'a': 1}],
            delimiter=',',
            format_name='CSV',
        )
        assert count == 1
        assert mod.read_delimited(
            file_path,
            delimiter=',',
        ) == [{'a': '1', 'b': '2'}]

    def test_warn_deprecated_module_io(self) -> None:
        """Test direct deprecation warning helper."""
        with pytest.warns(
            DeprecationWarning,
            match='etlplus.file.csv.read\\(\\) is deprecated',
        ):
            mod.warn_deprecated_module_io('etlplus.file.csv', 'read')

    def test_call_deprecated_module_read_and_write(
        self,
        tmp_path: Path,
    ) -> None:
        """Test deprecated wrapper delegates coerce paths and warn."""
        read_calls: list[Path] = []
        write_calls: list[tuple[Path, Any]] = []

        def _reader(path: Path) -> dict[str, object]:
            read_calls.append(path)
            return {'ok': True}

        def _writer(path: Path, data: Any) -> int:
            write_calls.append((path, data))
            return 3

        target = tmp_path / 'legacy.json'
        with pytest.warns(DeprecationWarning, match='deprecated'):
            read_result = mod.call_deprecated_module_read(
                str(target),
                'etlplus.file.json',
                _reader,
            )
        with pytest.warns(DeprecationWarning, match='deprecated'):
            write_result = mod.call_deprecated_module_write(
                str(target),
                {'x': 1},
                'etlplus.file.json',
                _writer,
            )

        assert read_result == {'ok': True}
        assert write_result == 3
        assert read_calls == [target]
        assert write_calls == [(target, {'x': 1})]
