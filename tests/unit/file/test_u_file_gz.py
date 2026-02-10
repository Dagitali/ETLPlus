"""
:mod:`tests.unit.file.test_u_file_gz` module.

Unit tests for :mod:`etlplus.file.gz`.
"""

from __future__ import annotations

import gzip
from pathlib import Path

import pytest

from etlplus.file import gz as mod
from etlplus.file.base import ReadOptions
from etlplus.file.base import WriteOptions
from tests.unit.file.conftest import ArchiveWrapperCoreDispatchModuleContract

# SECTION: TESTS ============================================================ #


class TestGz(ArchiveWrapperCoreDispatchModuleContract):
    """Unit tests for :mod:`etlplus.file.gz`."""

    module = mod
    format_name = 'gz'
    valid_path_name = 'payload.json.gz'
    missing_inner_path_name = 'payload.gz'
    expected_read_result = {'fmt': 'json', 'name': 'payload.json'}

    def assert_archive_payload(
        self,
        path: Path,
    ) -> None:
        """Assert gzip payload bytes for write contract tests."""
        with gzip.open(path, 'rb') as handle:
            assert handle.read() == b'payload'

    def seed_archive_payload(
        self,
        path: Path,
    ) -> None:
        """Seed gzip archive payload for read contract tests."""
        with gzip.open(path, 'wb') as handle:
            handle.write(b'payload')

    def test_read_inner_bytes_returns_payload(
        self,
        tmp_path: Path,
    ) -> None:
        """Test reading decompressed bytes via ``read_inner_bytes``."""
        path = tmp_path / 'payload.json.gz'
        expected = b'{"ok": true}'
        with gzip.open(path, 'wb') as handle:
            handle.write(expected)

        result = mod.GzFile().read_inner_bytes(
            path,
            options=ReadOptions(inner_name='ignored'),
        )

        assert result == expected

    def test_read_raises_on_missing_inner_format(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that reading a gzip file without an inner format fails."""
        path = tmp_path / 'payload.gz'

        with pytest.raises(ValueError, match='Cannot infer file format'):
            mod.read(path)

    def test_read_raises_on_non_gzip(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that reading a non-gzip file raises an error."""
        path = tmp_path / 'payload.json'
        path.write_text('irrelevant', encoding='utf-8')

        with pytest.raises(ValueError, match='Not a gzip file'):
            mod.read(path)

    def test_write_inner_bytes_writes_payload(
        self,
        tmp_path: Path,
    ) -> None:
        """Test writing compressed bytes via ``write_inner_bytes``."""
        path = tmp_path / 'payload.json.gz'
        payload = b'{"written": true}'

        mod.GzFile().write_inner_bytes(
            path,
            payload,
            options=WriteOptions(inner_name='ignored'),
        )

        with gzip.open(path, 'rb') as handle:
            assert handle.read() == payload

    def test_write_raises_on_missing_inner_format(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that writing without an inferable inner format fails."""
        path = tmp_path / 'payload.gz'

        with pytest.raises(ValueError, match='Cannot infer file format'):
            mod.write(path, [{'id': 1}])
