"""
:mod:`tests.unit.utils.test_u_utils_paths` module.

Unit tests for :mod:`etlplus.utils._paths`.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from etlplus.utils import PathHasher
from etlplus.utils import PathParser

# SECTION: TESTS ============================================================ #


class TestPathHasher:
    """Unit tests for path hashing helpers."""

    def test_sha256_returns_digest_for_existing_file(self, tmp_path: Path) -> None:
        """Existing files should hash to their SHA-256 digest."""
        path = tmp_path / 'pipeline.yml'
        path.write_text('name: pipeline-a\n', encoding='utf-8')

        assert PathHasher.sha256(path) == hashlib.sha256(path.read_bytes()).hexdigest()

    def test_sha256_returns_none_for_missing_path(self, tmp_path: Path) -> None:
        """Missing paths should not produce a digest."""
        assert PathHasher.sha256(tmp_path / 'missing.yml') is None


class TestPathParser:
    """Unit tests for path parsing helpers."""

    @pytest.mark.parametrize(
        ('value', 'expected'),
        [
            pytest.param('C:\\data\\file.csv', True, id='windows-backslash'),
            pytest.param('D:/data/file.csv', True, id='windows-forward-slash'),
            pytest.param('/tmp/data.csv', False, id='posix-absolute'),
            pytest.param('data/input.csv', False, id='relative'),
            pytest.param('s3://bucket/key', False, id='uri'),
            pytest.param('', False, id='empty'),
            pytest.param('1:/data', False, id='numeric-prefix'),
        ],
    )
    def test_is_windows_drive_path(self, value: str, expected: bool) -> None:
        """Test Windows drive-prefix detection."""
        assert PathParser.is_windows_drive_path(value) is expected
