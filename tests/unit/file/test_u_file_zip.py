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
    valid_path_name = 'payload.json.zip'
    missing_inner_path_name = 'payload.zip'
    expected_read_result = {'fmt': 'json', 'name': 'payload.json'}

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

    def test_read_inner_bytes_raises_on_unknown_inner_name(
        self,
        tmp_path: Path,
    ) -> None:
        """Test read_inner_bytes errors for unknown archive members."""
        path = self._archive_path(tmp_path, stem='payloads')
        _write_zip(path, {'a.json': b'first'})

        with pytest.raises(ValueError, match='ZIP archive member not found'):
            mod.ZipFile().read_inner_bytes(
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
        path = self._archive_path(tmp_path, stem='payloads')
        _write_zip(path, {'a.json': b'{}', 'b.json': b'{}'})

        with pytest.raises(ValueError, match='multiple members'):
            mod.ZipFile().read_inner_bytes(path)

    def test_read_inner_bytes_respects_inner_name_option(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that ``read_inner_bytes`` selects the configured member."""
        path = self._archive_path(tmp_path, stem='payloads')
        _write_zip(path, {'a.json': b'first', 'b.json': b'second'})

        result = mod.ZipFile().read_inner_bytes(
            path,
            options=ReadOptions(inner_name='b.json'),
        )

        assert result == b'second'

    def test_read_multiple_entries_raises_on_unknown_inner_name(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test that selecting an unknown archive member raises a clear error.
        """
        path = self._archive_path(tmp_path, stem='payloads')
        _write_zip(path, {'a.json': b'{}', 'b.json': b'{}'})

        with pytest.raises(ValueError, match='ZIP archive member not found'):
            mod.ZipFile().read(
                path,
                options=ReadOptions(inner_name='missing.json'),
            )

    def test_read_multiple_entries_respects_inner_name_option(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that reading a multi-entry archive with ``inner_name`` returns
        only the selected entry payload.
        """
        self.install_core_file_stub(monkeypatch)
        path = self._archive_path(tmp_path, stem='payloads')
        _write_zip(path, {'a.json': b'{}', 'b.json': b'{}'})

        result = mod.ZipFile().read(
            path,
            options=ReadOptions(inner_name='b.json'),
        )

        assert result == {'fmt': 'json', 'name': 'b.json'}

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
        path = self._archive_path(tmp_path, stem='payloads')
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
        path = self._archive_path(tmp_path, stem='empty')
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
        path = self._archive_path(tmp_path, stem='nested')
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
        path = self._archive_path(tmp_path, stem='payload')
        _write_zip(path, {'payload.unknown': b'ignored'})

        with pytest.raises(ValueError, match='Cannot infer file format'):
            mod.read(path)

    def test_write_raises_on_unexpected_output_compression(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that writing to non-ZIP compressed suffixes fails early."""
        self.install_core_file_stub(monkeypatch)
        path = self._archive_path(tmp_path, stem='payload.json', suffix='gz')

        with pytest.raises(ValueError, match='Unexpected compression'):
            mod.write(path, [{'id': 1}])

    def test_write_inner_bytes_uses_default_member_name(
        self,
        tmp_path: Path,
    ) -> None:
        """Test ``write_inner_bytes`` default archive member naming."""
        path = self._archive_path(tmp_path, stem='payload')

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
        path = self._archive_path(tmp_path, stem='payload.json')

        written = mod.ZipFile().write(
            path,
            [{'id': 1}],
            options=WriteOptions(inner_name='nested/payload.json'),
        )

        assert written == 1
        with zipfile.ZipFile(path, 'r') as archive:
            assert archive.namelist() == ['nested/payload.json']
            assert archive.read('nested/payload.json') == b'payload'

    @staticmethod
    def _archive_path(
        tmp_path: Path,
        *,
        stem: str,
        suffix: str = 'zip',
    ) -> Path:
        """Build deterministic archive paths for ad hoc test cases."""
        return tmp_path / f'{stem}.{suffix}'
