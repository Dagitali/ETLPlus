"""
:mod:`tests.unit.file.test_u_file_zip` module.

Unit tests for :mod:`etlplus.file.zip`.
"""

from __future__ import annotations

import zipfile
from pathlib import Path

import pytest

from etlplus.file import core
from etlplus.file import zip as mod
from etlplus.file.base import ReadOptions
from etlplus.file.base import WriteOptions
from etlplus.file.enums import FileFormat
from tests.unit.file.conftest import ArchiveWrapperCoreDispatchModuleContract

# SECTION: HELPERS ========================================================== #


class _StubFile:
    """Minimal stand-in for :class:`etlplus.file.core.File`."""

    # pylint: disable=unused-argument

    def __init__(
        self,
        path: Path,
        fmt: FileFormat,
    ) -> None:
        self.path = Path(path)
        self.fmt = fmt

    def read(self) -> dict[str, str]:
        """Return deterministic payload for smoke reads."""
        return {'fmt': self.fmt.value, 'name': self.path.name}

    def write(
        self,
        data: object,
    ) -> int:  # noqa: ARG002
        """Persist dummy bytes so ZIP write can read them."""
        self.path.write_text('payload', encoding='utf-8')
        return 1


def _write_zip(
    path: Path,
    entries: dict[str, bytes],
) -> None:
    """Helper to write a ZIP archive with specified entries."""
    with zipfile.ZipFile(
        path,
        'w',
        compression=zipfile.ZIP_DEFLATED,
    ) as archive:
        for name, payload in entries.items():
            archive.writestr(name, payload)


# SECTION: TESTS ============================================================ #


class TestZip(ArchiveWrapperCoreDispatchModuleContract):
    """Unit tests for :mod:`etlplus.file.zip`."""

    module = mod
    format_name = 'zip'
    valid_path_name = 'payload.json.zip'
    missing_inner_path_name = 'payload.zip'
    expected_read_result = {'fmt': 'json', 'name': 'payload.json'}

    def install_core_file_stub(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Install deterministic core file stub."""
        monkeypatch.setattr(core, 'File', _StubFile)

    def seed_archive_payload(
        self,
        path: Path,
    ) -> None:
        """Seed zip archive payload for read contract tests."""
        _write_zip(path, {'payload.json': b'{}'})

    def assert_archive_payload(
        self,
        path: Path,
    ) -> None:
        """Assert zip member payload for write contract tests."""
        with zipfile.ZipFile(path, 'r') as archive:
            assert archive.namelist() == ['payload.json']
            assert archive.read('payload.json') == b'payload'

    def test_read_multiple_entries_respects_inner_name_option(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that reading a multi-entry archive with ``inner_name`` returns
        only the selected entry payload.
        """
        monkeypatch.setattr(core, 'File', _StubFile)
        path = tmp_path / 'payloads.zip'
        _write_zip(path, {'a.json': b'{}', 'b.json': b'{}'})

        result = mod.ZipFile().read(
            path,
            options=ReadOptions(inner_name='b.json'),
        )

        assert result == {'fmt': 'json', 'name': 'b.json'}

    def test_read_multiple_entries_raises_on_unknown_inner_name(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test that selecting an unknown archive member raises a clear error.
        """
        path = tmp_path / 'payloads.zip'
        _write_zip(path, {'a.json': b'{}', 'b.json': b'{}'})

        with pytest.raises(ValueError, match='ZIP archive member not found'):
            mod.ZipFile().read(
                path,
                options=ReadOptions(inner_name='missing.json'),
            )

    def test_read_multiple_entries_returns_mapping(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that reading a ZIP archive with multiple entries returns a mapping
        of file names to their contents.
        """
        monkeypatch.setattr(core, 'File', _StubFile)
        path = tmp_path / 'payloads.zip'
        _write_zip(path, {'a.json': b'{}', 'b.json': b'{}'})

        result = mod.read(path)

        assert result == {
            'a.json': {'fmt': 'json', 'name': 'a.json'},
            'b.json': {'fmt': 'json', 'name': 'b.json'},
        }

    def test_read_raises_on_empty_archive(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that reading an empty ZIP archive raises an error."""
        path = tmp_path / 'empty.zip'
        _write_zip(path, {})

        with pytest.raises(ValueError, match='ZIP archive is empty'):
            mod.read(path)

    def test_read_raises_on_unexpected_compression(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test that reading a ZIP archive with unexpected compression raises an
        error.
        """
        path = tmp_path / 'nested.zip'
        _write_zip(path, {'data.csv.gz': b'ignored'})

        with pytest.raises(ValueError, match='Unexpected compression'):
            mod.read(path)

    def test_read_raises_on_unknown_inner_format(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test that reading a ZIP archive with unknown inner format raises an
        error.
        """
        path = tmp_path / 'payload.zip'
        _write_zip(path, {'payload.unknown': b'ignored'})

        with pytest.raises(ValueError, match='Cannot infer file format'):
            mod.read(path)

    def test_write_supports_nested_inner_name(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that writing supports nested archive member names."""
        monkeypatch.setattr(core, 'File', _StubFile)
        path = tmp_path / 'payload.json.zip'

        written = mod.ZipFile().write(
            path,
            [{'id': 1}],
            options=WriteOptions(inner_name='nested/payload.json'),
        )

        assert written == 1
        with zipfile.ZipFile(path, 'r') as archive:
            assert archive.namelist() == ['nested/payload.json']
            assert archive.read('nested/payload.json') == b'payload'
