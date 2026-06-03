"""
:mod:`tests.unit.storage.test_u_storage_local` module.

Unit tests for :mod:`etlplus.storage._local`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from etlplus.storage import LocalStorageBackend
from etlplus.storage import StorageLocation

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestLocalStorageBackend:
    """Unit tests for :class:`etlplus.storage.LocalStorageBackend`."""

    def test_delete_existing_directory(self, tmp_path: Path) -> None:
        """Test that delete removes empty directories."""
        target = tmp_path / 'folder'
        target.mkdir()
        backend = LocalStorageBackend()

        backend.delete(StorageLocation.from_value(target))

        assert target.exists() is False

    def test_delete_existing_file(self, tmp_path: Path) -> None:
        """Test that delete removes existing local files."""
        target = tmp_path / 'delete.txt'
        target.write_text('hello', encoding='utf-8')
        backend = LocalStorageBackend()

        backend.delete(StorageLocation.from_value(target))

        assert target.exists() is False

    def test_delete_missing_is_noop(self, tmp_path: Path) -> None:
        """Test that deleting a missing local file is a no-op."""
        target = tmp_path / 'missing.txt'
        backend = LocalStorageBackend()

        backend.delete(StorageLocation.from_value(target))

        assert target.exists() is False

    def test_exists(self, tmp_path: Path) -> None:
        """Test that :meth:`exists` reflects local filesystem state."""
        target = tmp_path / 'exists.txt'
        target.write_text('hello', encoding='utf-8')
        backend = LocalStorageBackend()
        assert backend.exists(StorageLocation.from_value(target)) is True

    def test_open_creates_parent_for_write_modes(self, tmp_path: Path) -> None:
        """Test that write modes create missing parent directories."""
        target = tmp_path / 'nested' / 'output.txt'
        backend = LocalStorageBackend()
        location = StorageLocation.from_value(target)

        with backend.open(location, 'w', encoding='utf-8') as handle:
            handle.write('payload')

        assert target.read_text(encoding='utf-8') == 'payload'

    def test_open_read_mode_skips_parent_creation(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that read mode does not try to create parent directories."""
        target = tmp_path / 'input.txt'
        target.write_text('payload', encoding='utf-8')
        backend = LocalStorageBackend()
        location = StorageLocation.from_value(target)
        created: list[bool] = []

        monkeypatch.setattr(
            backend,
            'ensure_parent_dir',
            lambda _location: created.append(True),
        )

        with backend.open(location, encoding='utf-8') as handle:
            assert handle.read() == 'payload'

        assert not created
