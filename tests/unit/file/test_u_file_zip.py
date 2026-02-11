"""
:mod:`tests.unit.file.test_u_file_zip` module.

Unit tests for :mod:`etlplus.file.zip`.
"""

from __future__ import annotations

import zipfile
from pathlib import Path

import pytest

from etlplus.file import zip as mod
from etlplus.file.base import ReadOptions
from etlplus.file.base import WriteOptions
from tests.unit.file.conftest import ArchiveWrapperCoreDispatchModuleContract

# SECTION: HELPERS ========================================================== #


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

    def assert_archive_payload(
        self,
        path: Path,
    ) -> None:
        """Assert zip member payload for write contract tests."""
        with zipfile.ZipFile(path, 'r') as archive:
            assert archive.namelist() == ['payload.json']
            assert archive.read('payload.json') == b'payload'

    def seed_archive_payload(
        self,
        path: Path,
    ) -> None:
        """Seed zip archive payload for read contract tests."""
        _write_zip(path, {'payload.json': b'{}'})

    @pytest.mark.parametrize(
        ('reader_method', 'needs_core_stub'),
        [('read_inner_bytes', False), ('read', True)],
        ids=['inner_bytes', 'parsed_read'],
    )
    def test_read_raises_on_unknown_inner_name(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        reader_method: str,
        needs_core_stub: bool,
    ) -> None:
        """Test read APIs erroring for unknown archive members."""
        if needs_core_stub:
            self.install_core_file_stub(monkeypatch)
        path = self.archive_path(tmp_path, stem='payloads')
        _write_zip(path, {'a.json': b'first', 'b.json': b'second'})

        with pytest.raises(ValueError, match='ZIP archive member not found'):
            getattr(mod.ZipFile(), reader_method)(
                path,
                options=ReadOptions(inner_name='missing.json'),
            )

    def test_read_inner_bytes_requires_inner_name_for_multiple_entries(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test that reading raw bytes from multi-member ZIP requires
        ``inner_name``.
        """
        path = self.archive_path(tmp_path, stem='payloads')
        _write_zip(path, {'a.json': b'{}', 'b.json': b'{}'})

        with pytest.raises(ValueError, match='multiple members'):
            mod.ZipFile().read_inner_bytes(path)

    @pytest.mark.parametrize(
        ('reader_method', 'needs_core_stub', 'entries', 'expected'),
        [
            (
                'read_inner_bytes',
                False,
                {'a.json': b'first', 'b.json': b'second'},
                b'second',
            ),
            (
                'read',
                True,
                {'a.json': b'{}', 'b.json': b'{}'},
                {'fmt': 'json', 'name': 'b.json'},
            ),
        ],
        ids=['inner_bytes', 'parsed_read'],
    )
    def test_read_respects_inner_name_option(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        reader_method: str,
        needs_core_stub: bool,
        entries: dict[str, bytes],
        expected: object,
    ) -> None:
        """Test read APIs selecting the configured archive member."""
        if needs_core_stub:
            self.install_core_file_stub(monkeypatch)
        path = self.archive_path(tmp_path, stem='payloads')
        _write_zip(path, entries)

        result = getattr(mod.ZipFile(), reader_method)(
            path,
            options=ReadOptions(inner_name='b.json'),
        )

        assert result == expected

    def test_read_multiple_entries_returns_mapping(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that reading a ZIP archive with multiple entries returns a mapping
        of file names to their contents.
        """
        self.install_core_file_stub(monkeypatch)
        path = self.archive_path(tmp_path, stem='payloads')
        _write_zip(path, {'a.json': b'{}', 'b.json': b'{}'})

        result = mod.ZipFile().read(path)

        assert result == {
            'a.json': {'fmt': 'json', 'name': 'a.json'},
            'b.json': {'fmt': 'json', 'name': 'b.json'},
        }

    @pytest.mark.parametrize(
        ('stem', 'entries', 'error_pattern'),
        [
            ('empty', {}, 'ZIP archive is empty'),
            ('nested', {'data.csv.gz': b'ignored'}, 'Unexpected compression'),
            (
                'payload',
                {'payload.unknown': b'ignored'},
                'Cannot infer file format',
            ),
        ],
        ids=[
            'empty_archive',
            'unexpected_compression',
            'unknown_inner_format',
        ],
    )
    def test_read_invalid_archives_raise(
        self,
        tmp_path: Path,
        stem: str,
        entries: dict[str, bytes],
        error_pattern: str,
    ) -> None:
        """Test invalid ZIP structures raising clear read-time errors."""
        path = self.archive_path(tmp_path, stem=stem)
        _write_zip(path, entries)

        with pytest.raises(ValueError, match=error_pattern):
            mod.ZipFile().read(path)

    def test_write_raises_on_unexpected_output_compression(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that writing to non-ZIP compressed suffixes fails early."""
        self.install_core_file_stub(monkeypatch)
        path = self.archive_path(tmp_path, stem='payload.json', suffix='gz')

        with pytest.raises(ValueError, match='Unexpected compression'):
            mod.ZipFile().write(path, [{'id': 1}])

    def test_write_inner_bytes_uses_default_member_name(
        self,
        tmp_path: Path,
    ) -> None:
        """Test ``write_inner_bytes`` default archive member naming."""
        path = self.archive_path(tmp_path, stem='payload')

        mod.ZipFile().write_inner_bytes(path, b'data')

        with zipfile.ZipFile(path, 'r') as archive:
            assert archive.namelist() == ['payload']
            assert archive.read('payload') == b'data'

    def test_write_supports_nested_inner_name(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that writing supports nested archive member names."""
        self.install_core_file_stub(monkeypatch)
        path = self.archive_path(tmp_path, stem='payload.json')

        written = mod.ZipFile().write(
            path,
            [{'id': 1}],
            options=WriteOptions(inner_name='nested/payload.json'),
        )

        assert written == 1
        with zipfile.ZipFile(path, 'r') as archive:
            assert archive.namelist() == ['nested/payload.json']
            assert archive.read('nested/payload.json') == b'payload'
