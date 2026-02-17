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

from .pytest_file_contracts import ArchiveWrapperCoreDispatchModuleContract

# SECTION: TESTS ============================================================ #


class TestGz(ArchiveWrapperCoreDispatchModuleContract):
    """Unit tests for :mod:`etlplus.file.gz`."""

    module = mod
    format_name = 'gz'

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
        path = self.archive_path(tmp_path, stem='payload.json')
        expected = b'{"ok": true}'
        with gzip.open(path, 'wb') as handle:
            handle.write(expected)

        result = mod.GzFile().read_inner_bytes(
            path,
            options=ReadOptions(inner_name='ignored'),
        )

        assert result == expected

    @pytest.mark.parametrize(
        ('stem', 'suffix', 'text_content', 'error_pattern'),
        [
            ('payload', None, None, 'Cannot infer file format'),
            ('payload', 'json', 'irrelevant', 'Not a gzip file'),
        ],
        ids=['missing_inner_format', 'non_gzip_payload'],
    )
    def test_read_invalid_inputs_raise(
        self,
        tmp_path: Path,
        stem: str,
        suffix: str | None,
        text_content: str | None,
        error_pattern: str,
    ) -> None:
        """Test invalid gzip read inputs raising clear errors."""
        if suffix is None:
            path = self.archive_path(tmp_path, stem=stem)
        else:
            path = self.archive_path(tmp_path, stem=stem, suffix=suffix)
        if text_content is not None:
            path.write_text(text_content, encoding='utf-8')

        with pytest.raises(ValueError, match=error_pattern):
            mod.GzFile().read(path)

    def test_write_inner_bytes_writes_payload(
        self,
        tmp_path: Path,
    ) -> None:
        """Test writing compressed bytes via ``write_inner_bytes``."""
        path = self.archive_path(tmp_path, stem='payload.json')
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
        path = self.archive_path(tmp_path, stem='payload')

        with pytest.raises(ValueError, match='Cannot infer file format'):
            mod.GzFile().write(path, [{'id': 1}])
