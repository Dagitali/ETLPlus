"""
:mod:`tests.unit.file.test_u_file_stub_categories` module.

Unit tests for :mod:`etlplus.file._stub_categories`.
"""

from __future__ import annotations

from pathlib import Path
from typing import cast

import pytest

from etlplus.file import FileFormat
from etlplus.file import _stub_categories as mod
from etlplus.file.base import ReadOptions
from etlplus.file.base import WriteOptions
from etlplus.file.stub import StubFileHandlerABC
from etlplus.types import JSONData

# SECTION: HELPERS ========================================================== #


def _patch_stub_read(
    monkeypatch: pytest.MonkeyPatch,
    *,
    result: JSONData,
    expected_path: Path | None = None,
) -> list[tuple[Path, ReadOptions | None]]:
    """Patch ``StubFileHandlerABC.read`` and collect read call metadata."""
    read_calls: list[tuple[Path, ReadOptions | None]] = []

    def _read(
        self: StubFileHandlerABC,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        if expected_path is not None:
            assert path == expected_path
        read_calls.append((path, options))
        return result

    monkeypatch.setattr(StubFileHandlerABC, 'read', _read)
    return read_calls


def _patch_stub_write(
    monkeypatch: pytest.MonkeyPatch,
    *,
    result: int,
) -> list[tuple[Path, JSONData, WriteOptions | None]]:
    """Patch ``StubFileHandlerABC.write`` and collect write call metadata."""
    write_calls: list[tuple[Path, JSONData, WriteOptions | None]] = []

    def _write(
        self: StubFileHandlerABC,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        write_calls.append((path, data, options))
        return result

    monkeypatch.setattr(StubFileHandlerABC, 'write', _write)
    return write_calls


class _BinaryStub(mod.StubBinarySerializationFileHandlerABC):
    """Concrete binary stub for unit tests."""

    format = FileFormat.CBOR


class _EmbeddedStub(mod.StubEmbeddedDatabaseFileHandlerABC):
    """Concrete embedded-database stub for unit tests."""

    format = FileFormat.ACCDB


class _LogStub(mod.StubLogEventFileHandlerABC):
    """Concrete log-event stub for unit tests."""

    format = FileFormat.LOG


class _SemiStructuredStub(mod.StubSemiStructuredTextFileHandlerABC):
    """Concrete semi-structured stub for unit tests."""

    format = FileFormat.CONF


class _SingleScientificStub(mod.StubSingleDatasetScientificFileHandlerABC):
    """Concrete single-dataset scientific stub for unit tests."""

    format = FileFormat.MAT


class _SpreadsheetStub(mod.StubSpreadsheetFileHandlerABC):
    """Concrete spreadsheet stub for unit tests."""

    format = FileFormat.WKS


class _TemplateStub(mod.StubTemplateFileHandlerABC):
    """Concrete template stub for unit tests."""

    format = FileFormat.JINJA2


# SECTION: TESTS ============================================================ #


class TestStubCategoryHandlers:
    """Unit tests for category-specific stub handler ABCs."""

    # pylint: disable=protected-access

    def test_binary_stub_loads_and_dumps_delegate_to_stub_io(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test binary payload methods delegating to StubFileHandlerABC."""
        read_calls = _patch_stub_read(
            monkeypatch,
            result=cast(JSONData, [{'id': 1}]),
        )
        write_calls = _patch_stub_write(monkeypatch, result=1)
        handler = _BinaryStub()

        assert handler.loads_bytes(b'payload') == [{'id': 1}]
        assert handler.dumps_bytes([{'id': 1}]) == b''
        assert read_calls == [(Path('ignored.cbor'), None)]
        assert write_calls == [(Path('ignored.cbor'), [{'id': 1}], None)]

    def test_embedded_stub_methods_delegate_to_stub_io(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test embedded-database methods delegating to StubFileHandlerABC."""
        source_path = Path('data.accdb')
        read_calls = _patch_stub_read(
            monkeypatch,
            result=cast(JSONData, [{'id': 1}]),
        )
        write_calls = _patch_stub_write(monkeypatch, result=3)
        handler = _EmbeddedStub()

        assert handler.connect(source_path) == [{'id': 1}]
        assert handler.list_tables(object()) == [{'id': 1}]
        assert handler.read(source_path) == [{'id': 1}]
        assert handler.read_table(object(), 'events') == [{'id': 1}]
        assert handler.write(source_path, [{'id': 1}]) == 3
        assert handler.write_table(object(), 'events', [{'id': 1}]) == 3
        assert [path for path, _ in read_calls] == [
            source_path,
            Path('ignored.accdb'),
            source_path,
            Path('ignored.accdb'),
        ]
        assert [
            (path, data)
            for path, data, _ in write_calls
        ] == [
            (source_path, cast(JSONData, [{'id': 1}])),
            (Path('ignored.accdb'), cast(JSONData, [{'id': 1}])),
        ]

    def test_log_stub_parse_and_serialize_delegate_to_stub_io(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test log-event parse/serialize methods delegating to stub IO."""
        _ = _patch_stub_read(
            monkeypatch,
            result=cast(JSONData, {'event': 'ok'}),
            expected_path=Path('ignored.log'),
        )
        write_calls = _patch_stub_write(monkeypatch, result=1)
        handler = _LogStub()

        assert handler.parse_line('hello') == {'event': 'ok'}
        assert handler.serialize_event({'event': 'ok'}) == ''
        assert [
            (path, data)
            for path, data, _ in write_calls
        ] == [(Path('ignored.log'), {'event': 'ok'})]

    def test_semistructured_stub_methods_delegate_to_stub_io(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test semi-structured stub read/write/loads/dumps delegation."""
        source_path = Path('data.conf')
        read_calls = _patch_stub_read(
            monkeypatch,
            result=cast(JSONData, {'ok': True}),
        )
        write_calls = _patch_stub_write(monkeypatch, result=2)
        handler = _SemiStructuredStub()

        assert handler.read(source_path) == {'ok': True}
        assert handler.loads('ignored') == {'ok': True}
        assert handler.write(source_path, {'ok': True}) == 2
        assert handler.dumps({'ok': True}) == ''
        assert [path for path, _ in read_calls] == [
            source_path,
            Path('ignored.conf'),
        ]
        assert [
            (path, data)
            for path, data, _ in write_calls
        ] == [
            (source_path, {'ok': True}),
            (Path('ignored.conf'), {'ok': True}),
        ]

    def test_single_dataset_stub_validates_dataset_key(
        self,
    ) -> None:
        """Test single-dataset stub rejecting non-default dataset keys."""
        handler = _SingleScientificStub()
        path = Path('data.mat')
        with pytest.raises(ValueError, match='supports only dataset key'):
            handler.read_dataset(path, dataset='other')
        with pytest.raises(ValueError, match='supports only dataset key'):
            handler.write_dataset(path, [], dataset='other')

    def test_single_dataset_stub_read_write_delegate_for_default_dataset(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test single-dataset stub read/write delegation for valid dataset."""
        read_calls = _patch_stub_read(
            monkeypatch,
            result=cast(JSONData, [{'id': 1}]),
        )
        write_calls = _patch_stub_write(monkeypatch, result=1)
        handler = _SingleScientificStub()
        path = Path('data.mat')
        read_options = ReadOptions(dataset='data')
        write_options = WriteOptions(dataset='data')

        assert handler.read(path, options=read_options) == [{'id': 1}]
        assert handler.write(path, [{'id': 1}], options=write_options) == 1
        assert read_calls == [(path, read_options)]
        assert write_calls == [(path, [{'id': 1}], write_options)]

    def test_spreadsheet_stub_methods_delegate_to_stub_io(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test spreadsheet stub methods forwarding to StubFileHandlerABC."""
        path = Path('book.wks')
        read_calls = _patch_stub_read(
            monkeypatch,
            result=cast(JSONData, [{'id': 1}]),
        )
        write_calls = _patch_stub_write(monkeypatch, result=4)
        handler = _SpreadsheetStub()

        assert handler.engine_name == 'stub'
        assert handler.read(path) == [{'id': 1}]
        assert handler.read_sheet(path, sheet='Sheet1') == [{'id': 1}]
        assert handler.write(path, [{'id': 1}]) == 4
        assert handler.write_sheet(path, [{'id': 1}], sheet='Sheet1') == 4
        assert [read_path for read_path, _ in read_calls] == [path, path]
        assert [
            (write_path, write_data)
            for write_path, write_data, _ in write_calls
        ] == [
            (path, cast(JSONData, [{'id': 1}])),
            (path, cast(JSONData, [{'id': 1}])),
        ]

    def test_template_stub_render_delegates_to_stub_read(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test template stub rendering delegating to stub read behavior."""
        _ = _patch_stub_read(
            monkeypatch,
            result=cast(JSONData, 'rendered'),
            expected_path=Path('ignored.jinja2'),
        )
        handler = _TemplateStub()

        assert handler.template_engine == 'stub'
        assert handler.render('Hi {{ name }}', {'name': 'Ada'}) == 'rendered'
