"""
:mod:`tests.unit.storage.test_u_storage_location` module.

Unit tests for :mod:`etlplus.storage._location`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from etlplus.storage import StorageLocation
from etlplus.storage import StorageScheme

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestStorageLocation:
    """Unit tests for :class:`etlplus.storage.StorageLocation`."""

    def test_from_path_object(self, tmp_path: Path) -> None:
        """Test that :class:`Path` inputs are preserved as local locations."""
        target = tmp_path / 'sample.json'
        location = StorageLocation.from_value(target)
        assert location.scheme is StorageScheme.FILE
        assert location.as_path() == target

    def test_from_scheme_alias_uri(
        self,
        raw: str,
        scheme: StorageScheme,
    ) -> None:
        """Test that known remote scheme aliases normalize correctly."""
        location = StorageLocation.from_value(raw)
        assert location.scheme is scheme

    @pytest.mark.parametrize(
        ('raw', 'scheme', 'authority', 'path', 'as_path'),
        [
            (
                'abfs://filesystem@example.dfs.core.windows.net/path/to/blob.parquet',
                StorageScheme.ABFS,
                'filesystem@example.dfs.core.windows.net',
                'path/to/blob.parquet',
                None,
            ),
            (
                'azure-blob://container/path/to/blob.json',
                StorageScheme.AZURE_BLOB,
                'container',
                'path/to/blob.json',
                None,
            ),
            (
                'https://example.dfs.core.windows.net/filesystem/path/to/blob.parquet',
                StorageScheme.ABFS,
                'filesystem@example.dfs.core.windows.net',
                'path/to/blob.parquet',
                None,
            ),
            (
                'https://example.blob.core.windows.net/container/path/to/blob.json',
                StorageScheme.AZURE_BLOB,
                'container@example.blob.core.windows.net',
                'path/to/blob.json',
                None,
            ),
            (
                'https://example.com/files/data.csv?download=1',
                StorageScheme.HTTP,
                'example.com',
                'files/data.csv',
                None,
            ),
            (
                'file:///tmp/example.csv',
                StorageScheme.FILE,
                '',
                '/tmp/example.csv',
                Path('/tmp/example.csv'),
            ),
            (
                'data/input.csv',
                StorageScheme.FILE,
                '',
                'data/input.csv',
                Path('data/input.csv'),
            ),
            (
                's3://bucket/path/to/object.parquet',
                StorageScheme.S3,
                'bucket',
                'path/to/object.parquet',
                None,
            ),
        ],
    )
    def test_from_value_parses_storage_location(
        self,
        raw: str,
        scheme: StorageScheme,
        authority: str,
        path: str,
        as_path: Path | None,
    ) -> None:
        """Test that storage location strings normalize into parsed parts."""
        location = StorageLocation.from_value(raw)
        assert location.scheme is scheme
        assert location.authority == authority
        assert location.path == path
        assert location.is_local is (scheme is StorageScheme.FILE)
        assert location.is_remote is (scheme is not StorageScheme.FILE)
        if as_path is not None:
            assert location.as_path() == as_path

    @pytest.mark.parametrize(
        ('raw', 'scheme'),
        [
            (
                'abfss://filesystem@example.dfs.core.windows.net/data.parquet',
                StorageScheme.ABFS,
            ),
            ('https://example.com/files/data.csv', StorageScheme.HTTP),
            ('wasbs://container/path/to/blob.json', StorageScheme.AZURE_BLOB),
            ('s3a://bucket/data.json', StorageScheme.S3),
            ('webhdfs://namenode.example.com/data.json', StorageScheme.HDFS),
        ],
    )
    def test_from_value_rejects_empty_input(self) -> None:
        """Test that blank location inputs are rejected."""
        with pytest.raises(ValueError, match='cannot be empty'):
            StorageLocation.from_value('  ')

    def test_remote_location_as_path_raises(self) -> None:
        """Test that remote locations cannot be converted to local paths."""
        location = StorageLocation.from_value('ftp://example.com/export.csv')
        with pytest.raises(TypeError, match='Only local storage locations'):
            location.as_path()
