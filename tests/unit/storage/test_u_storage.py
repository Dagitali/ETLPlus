"""
:mod:`tests.unit.storage.test_u_storage` module.

Unit tests for :mod:`etlplus.storage`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from etlplus.storage import AzureBlobStorageBackend
from etlplus.storage import LocalStorageBackend
from etlplus.storage import S3StorageBackend
from etlplus.storage import StorageLocation
from etlplus.storage import StorageScheme
from etlplus.storage import coerce_location
from etlplus.storage import get_backend

# SECTION: TESTS ============================================================ #


class TestAzureBlobStorageBackend:
    """Unit tests for :class:`etlplus.storage.AzureBlobStorageBackend`."""

    def test_exists_raises_placeholder_error(self) -> None:
        """Test that Azure Blob runtime operations are placeholders."""
        backend = AzureBlobStorageBackend()
        location = StorageLocation.from_value(
            'azure-blob://container/path/to/blob.json',
        )
        with pytest.raises(
            NotImplementedError,
            match='azure-storage-blob-backed',
        ):
            backend.exists(location)

    def test_ensure_parent_dir_validates_container_and_blob(self) -> None:
        """Test that invalid Azure Blob locations fail validation early."""
        backend = AzureBlobStorageBackend()
        location = StorageLocation.from_value('azure-blob://container')
        with pytest.raises(ValueError, match='blob path'):
            backend.ensure_parent_dir(location)


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


class TestS3StorageBackend:
    """Unit tests for :class:`etlplus.storage.S3StorageBackend`."""

    def test_ensure_parent_dir_validates_bucket_and_key(self) -> None:
        """Test that invalid S3 locations fail validation early."""
        backend = S3StorageBackend()
        location = StorageLocation.from_value('s3://bucket')
        with pytest.raises(ValueError, match='object key'):
            backend.ensure_parent_dir(location)

    def test_exists_raises_placeholder_error(self) -> None:
        """Test that runtime S3 operations are explicit placeholders."""
        backend = S3StorageBackend()
        location = StorageLocation.from_value('s3://bucket/data.json')
        with pytest.raises(NotImplementedError, match='boto3-backed'):
            backend.exists(location)


class TestStorageLocation:
    """Unit tests for :class:`etlplus.storage.StorageLocation`."""

    def test_from_azure_blob_uri(self) -> None:
        """Test that Azure Blob URIs keep container and blob path segments."""
        location = StorageLocation.from_value(
            'azure-blob://container/path/to/blob.json',
        )
        assert location.scheme is StorageScheme.AZURE_BLOB
        assert location.authority == 'container'
        assert location.path == 'path/to/blob.json'

    def test_from_file_uri(self) -> None:
        """Test that ``file://`` URIs normalize to local file locations."""
        location = StorageLocation.from_value('file:///tmp/example.csv')
        assert location.scheme is StorageScheme.FILE
        assert location.path == '/tmp/example.csv'
        assert location.as_path() == Path('/tmp/example.csv')

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

    def test_remote_location_as_path_raises(self) -> None:
        """Test that remote locations cannot be converted to local paths."""
        location = StorageLocation.from_value('ftp://example.com/export.csv')
        with pytest.raises(TypeError, match='Only local storage locations'):
            location.as_path()

    def test_from_remote_uri(self) -> None:
        """Test that remote URIs keep scheme, authority, and relative path."""
        location = StorageLocation.from_value(
            's3://bucket/path/to/object.parquet',
        )
        assert location.scheme is StorageScheme.S3
        assert location.authority == 'bucket'
        assert location.path == 'path/to/object.parquet'
        assert location.is_remote is True


class TestStorageRegistry:
    """Unit tests for storage backend resolution helpers."""

    def test_coerce_location_preserves_existing_instance(self) -> None:
        """
        Test that :func:`coerce_location` returns existing instances as-is.
        """
        location = StorageLocation.from_value('data/input.csv')
        assert coerce_location(location) is location

    def test_get_backend_for_azure_blob_location(self) -> None:
        """Test that Azure Blob locations resolve to the Azure backend."""
        backend = get_backend('azure-blob://container/path.json')
        assert isinstance(backend, AzureBlobStorageBackend)

    def test_get_backend_for_local_location(self) -> None:
        """Test that local storage resolves to the local backend."""
        backend = get_backend('data/input.csv')
        assert isinstance(backend, LocalStorageBackend)

    def test_get_backend_for_s3_location(self) -> None:
        """Test that S3 locations resolve to the S3 backend skeleton."""
        backend = get_backend('s3://bucket/path.json')
        assert isinstance(backend, S3StorageBackend)

    def test_get_backend_for_unsupported_remote_location_raises(self) -> None:
        """Test that unsupported remote schemes still raise clearly."""
        with pytest.raises(NotImplementedError, match="'ftp'"):
            get_backend('ftp://example.com/path.json')
