"""
:mod:`tests.unit.file.test_u_file_io` module.

Unit tests for :mod:`etlplus.file._io`.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from etlplus.file import _io as mod

from .pytest_file_support import RecordsFrameStub
from .pytest_file_support import RemoteBytesBackendStub
from .pytest_file_support import RemoteTextBackendStub

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: INTERNAL CLASSES ================================================= #


class _PandasReadSasStub:
    """Minimal pandas-like stub for ``read_sas`` helper tests."""

    def __init__(
        self,
        *,
        capture_payload: bool = False,
    ) -> None:
        self.calls: list[dict[str, object]] = []
        self.capture_payload = capture_payload

    def read_sas(self, path: Path, **kwargs: object) -> object:
        """Capture calls and return a sentinel object."""
        call = {'path': path, **kwargs}
        if self.capture_payload:
            call['payload'] = path.read_bytes()
        self.calls.append(call)
        return {'ok': True}


# SECTION: TESTS ============================================================ #


class TestIoHelpers:
    """Unit tests for shared file IO helpers."""

    def test_close_connection_noop_when_close_is_not_callable(self) -> None:
        """
        Test connection cleanup no-op when ``close`` is non-callable.
        """
        connection = type('_Conn', (), {'close': 1})()
        mod.EmbeddedDatabaseTableOption().close_connection(connection)

    def test_coerce_path_accepts_str_and_path(self, tmp_path: Path) -> None:
        """
        Test path coercion from strings and existing :class:`Path` objects.
        """
        value = tmp_path / 'file.txt'
        assert mod.coerce_path(str(value)) == value
        assert mod.coerce_path(value) == value

    def test_coerce_record_payload_contract(self) -> None:
        """Test record payload coercion for supported and invalid shapes."""
        for valid_payload, expected in (
            ({'a': 1}, {'a': 1}),
            ([{'a': 1}, {'a': 2}], [{'a': 1}, {'a': 2}]),
        ):
            assert (
                mod.coerce_record_payload(valid_payload, format_name='JSON') == expected
            )

        invalid_payload_cases: tuple[tuple[Any, str], ...] = (
            ([{'a': 1}, 2], 'array must contain only objects'),
            ('bad', 'root must be an object'),
        )
        for invalid_payload, pattern in invalid_payload_cases:
            with pytest.raises(TypeError, match=pattern):
                mod.coerce_record_payload(
                    invalid_payload,
                    format_name='JSON',
                )

    def test_normalize_records_contract(self) -> None:
        """Test record normalization for valid and invalid payloads."""
        for valid_payload, expected in (
            ({'id': 1}, [{'id': 1}]),
            ([{'id': 1}, {'id': 2}], [{'id': 1}, {'id': 2}]),
        ):
            assert mod.normalize_records(valid_payload, 'CSV') == expected

        invalid_payload_cases: tuple[tuple[Any, str], ...] = (
            (1, 'must be an object or an array'),
            ([{'id': 1}, 'x'], 'contain only objects'),
        )
        for invalid_payload, pattern in invalid_payload_cases:
            with pytest.raises(TypeError, match=pattern):
                mod.normalize_records(invalid_payload, 'CSV')

    def test_read_and_write_bytes_support_remote_locations(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test binary helpers against one storage-backed remote location."""
        backend = RemoteBytesBackendStub(read_payload=b'payload')
        monkeypatch.setattr(mod, 'get_backend', lambda location: backend)

        uri = 's3://bucket/data.bin'
        mod.write_bytes(uri, b'payload')

        assert backend.calls == ['ensure_parent_dir', 'wb']
        assert backend.uploads == [b'payload']
        assert mod.read_bytes(uri) == b'payload'
        assert backend.calls == ['ensure_parent_dir', 'wb', 'rb']

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

    def test_read_and_write_delimited_support_remote_locations(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test delimited helpers against one storage-backed remote location."""
        backend = RemoteTextBackendStub(read_payload='a,b\n1,2\n')
        monkeypatch.setattr(mod, 'get_backend', lambda location: backend)

        uri = 's3://bucket/rows.csv'
        count = mod.write_delimited(
            uri,
            [{'b': 2, 'a': 1}],
            delimiter=',',
            format_name='CSV',
        )

        assert count == 1
        assert backend.calls == ['ensure_parent_dir', 'w']
        assert backend.uploads == ['a,b\r\n1,2\r\n']
        assert mod.read_delimited(uri, delimiter=',') == [{'a': '1', 'b': '2'}]
        assert backend.calls == ['ensure_parent_dir', 'w', 'r']

    def test_read_and_write_text(
        self,
        tmp_path: Path,
    ) -> None:
        """Test text writing/reading, including trailing newline behavior."""
        file_path = tmp_path / 'out' / 'data.txt'
        mod.write_text(file_path, 'line', trailing_newline=True)
        assert file_path.read_text(encoding='utf-8') == 'line\n'
        assert mod.read_text(file_path) == 'line\n'

    def test_read_and_write_text_support_remote_locations(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test text helpers against one storage-backed remote location."""
        backend = RemoteTextBackendStub(read_payload='line\n')
        monkeypatch.setattr(mod, 'get_backend', lambda location: backend)

        uri = 's3://bucket/data.txt'
        mod.write_text(uri, 'line', trailing_newline=True)

        assert backend.calls == ['ensure_parent_dir', 'w']
        assert backend.uploads == ['line\n']
        assert mod.read_text(uri) == 'line\n'
        assert backend.calls == ['ensure_parent_dir', 'w', 'r']

    def test_read_sas_table_without_format_hint_omits_format_kwarg(
        self,
        tmp_path: Path,
    ) -> None:
        """Test SAS helper read path when no format hint is provided."""
        pandas = _PandasReadSasStub()
        path = tmp_path / 'sample.sas7bdat'

        result = mod.read_sas_table(
            pandas,
            path,
            format_hint=None,
        )

        assert result == {'ok': True}
        assert pandas.calls == [{'path': path}]

    def test_read_sas_table_stages_remote_input_to_local_path(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test SAS helper staging remote input before pandas reads it."""
        payload = b'remote-sas-payload'
        pandas = _PandasReadSasStub(capture_payload=True)
        backend = RemoteBytesBackendStub(read_payload=payload)
        monkeypatch.setattr(mod, 'get_backend', lambda location: backend)

        result = mod.read_sas_table(
            pandas,
            's3://bucket/sample.sas7bdat',
            format_hint='sas7bdat',
        )

        assert result == {'ok': True}
        assert len(pandas.calls) == 1
        staged_path = pandas.calls[0]['path']
        assert isinstance(staged_path, Path)
        assert staged_path.name == 'sample.sas7bdat'
        assert pandas.calls[0]['payload'] == payload
        assert pandas.calls[0]['format'] == 'sas7bdat'
        assert backend.calls == ['rb']

    def test_records_from_table(self) -> None:
        """Test conversion from dataframe-like objects."""
        table = RecordsFrameStub([{'id': 1}, {'id': 2}])
        assert mod.records_from_table(table) == [{'id': 1}, {'id': 2}]

    def test_stringify_value(self) -> None:
        """Test scalar stringification rules."""
        for value, expected in ((None, ''), (12, '12'), ('abc', 'abc')):
            assert mod.stringify_value(value) == expected
