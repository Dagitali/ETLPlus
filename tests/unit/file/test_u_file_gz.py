"""
:mod:`tests.unit.file.test_u_file_gz` module.

Unit tests for :mod:`etlplus.file.gz`.
"""

from __future__ import annotations

import gzip
from pathlib import Path

import pytest

from etlplus.file import core
from etlplus.file import gz as mod
from tests.unit.file.conftest import ArchiveWrapperCoreDispatchModuleContract
from tests.unit.file.conftest import CoreDispatchFileStub

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

    def install_core_file_stub(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Install deterministic core file stub."""
        monkeypatch.setattr(core, 'File', CoreDispatchFileStub)

    def seed_archive_payload(
        self,
        path: Path,
    ) -> None:
        """Seed gzip archive payload for read contract tests."""
        with gzip.open(path, 'wb') as handle:
            handle.write(b'payload')

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
