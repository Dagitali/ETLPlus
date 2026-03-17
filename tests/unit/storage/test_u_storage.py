"""
:mod:`tests.unit.storage.test_u_storage` module.

Unit tests for :mod:`etlplus.storage`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from etlplus.storage import LocalStorageBackend
from etlplus.storage import StorageLocation
from etlplus.storage import StorageScheme
from etlplus.storage import coerce_location
from etlplus.storage import get_backend

# SECTION: TESTS ============================================================ #


class TestStorageLocation:
    """Unit tests for :class:`etlplus.storage.StorageLocation`."""

    def test_from_local_path_string(self) -> None:
        """Test that plain paths are treated as local storage."""
        location = StorageLocation.from_value('data/input.csv')
        assert location.scheme is StorageScheme.FILE
        assert location.path == 'data/input.csv'
        assert location.is_local is True

    def test_from_path_object(self, tmp_path: Path) -> None:
        """Test that :class:`Path` inputs are preserved as local locations."""
        target = tmp_path / 'sample.json'
        location = StorageLocation.from_value(target)
        assert location.scheme is StorageScheme.FILE
        assert location.as_path() == target

    def test_from_file_uri(self) -> None:
        """Test that ``file://`` URIs normalize to local file locations."""
        location = StorageLocation.from_value('file:///tmp/example.csv')
        assert location.scheme is StorageScheme.FILE
        assert location.path == '/tmp/example.csv'
        assert location.as_path() == Path('/tmp/example.csv')

    def test_from_remote_uri(self) -> None:
        """Test that remote URIs keep scheme, authority, and relative path."""
        location = StorageLocation.from_value(
            's3://bucket/path/to/object.parquet',
        )
        assert location.scheme is StorageScheme.S3
        assert location.authority == 'bucket'
        assert location.path == 'path/to/object.parquet'
        assert location.is_remote is True

    def test_remote_location_as_path_raises(self) -> None:
        """Test that remote locations cannot be converted to local paths."""
        location = StorageLocation.from_value('ftp://example.com/export.csv')
        with pytest.raises(TypeError, match='Only local storage locations'):
            location.as_path()


class TestStorageRegistry:
    """Unit tests for storage backend resolution helpers."""

    def test_coerce_location_preserves_existing_instance(self) -> None:
        """
        Test that :func:`coerce_location` returns existing instances as-is.
        """
        location = StorageLocation.from_value('data/input.csv')
        assert coerce_location(location) is location

    def test_get_backend_for_local_location(self) -> None:
        """Test that local storage resolves to the local backend."""
        backend = get_backend('data/input.csv')
        assert isinstance(backend, LocalStorageBackend)

    def test_get_backend_for_remote_location_raises(self) -> None:
        """Test that unimplemented remote schemes raise a clear error."""
        with pytest.raises(NotImplementedError, match="'s3'"):
            get_backend('s3://bucket/path.json')


class TestLocalStorageBackend:
    """Unit tests for :class:`etlplus.storage.LocalStorageBackend`."""

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
