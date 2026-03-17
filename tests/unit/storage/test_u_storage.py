"""
:mod:`tests.unit.storage.test_u_storage` module.

Unit tests for :mod:`etlplus.storage`.
"""

from __future__ import annotations

from io import BytesIO
from pathlib import Path

import pytest

from etlplus.storage import AbfsStorageBackend
from etlplus.storage import AzureBlobStorageBackend
from etlplus.storage import FtpStorageBackend
from etlplus.storage import LocalStorageBackend
from etlplus.storage import RemoteStorageBackend
from etlplus.storage import S3StorageBackend
from etlplus.storage import StorageLocation
from etlplus.storage import StorageScheme
from etlplus.storage import StubStorageBackend
from etlplus.storage import abfs as abfs_mod
from etlplus.storage import azure_blob as azure_blob_mod
from etlplus.storage import coerce_location
from etlplus.storage import get_backend

# SECTION: TESTS ============================================================ #


class TestAbfsStorageBackend:
    """Unit tests for :class:`etlplus.storage.AbfsStorageBackend`."""

    def test_exists_raises_import_error_without_sdk(self) -> None:
        """Test that ABFS runtime needs the optional SDK package."""
        backend = AbfsStorageBackend()
        location = StorageLocation.from_value(
            'abfs://filesystem@example.dfs.core.windows.net/data.parquet',
        )
        with pytest.raises(
            ImportError,
            match='azure-storage-file-datalake',
        ):
            backend.exists(location)

    def test_exists_uses_file_client(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that ABFS exists delegates to the file client."""
        backend = AbfsStorageBackend()
        location = StorageLocation.from_value(
            'abfs://filesystem@example.dfs.core.windows.net/blob.json',
        )

        class FakeFileClient:
            """Data Lake file client test double."""

            def exists(self) -> bool:
                """Return a present-file result."""
                return True

        def fake_file_client(_location: StorageLocation) -> FakeFileClient:
            return FakeFileClient()

        monkeypatch.setattr(backend, '_file_client', fake_file_client)
        assert backend.exists(location) is True

    def test_inherits_remote_storage_backend_base(self) -> None:
        """Test that ABFS uses the shared remote backend base class."""
        assert issubclass(AbfsStorageBackend, RemoteStorageBackend)
        assert not issubclass(AbfsStorageBackend, StubStorageBackend)

    def test_open_reads_text_payload(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that ABFS reads return text buffers when requested."""
        backend = AbfsStorageBackend()
        location = StorageLocation.from_value(
            'abfs://filesystem@example.dfs.core.windows.net/blob.json',
        )

        class FakeDownload:
            """Data Lake download test double."""

            def readall(self) -> bytes:
                """Return a fixed payload."""
                return b'{"ok": true}'

        class FakeFileClient:
            """Data Lake file client test double."""

            def download_file(self) -> FakeDownload:
                """Return the fake download wrapper."""
                return FakeDownload()

        def fake_file_client(_location: StorageLocation) -> FakeFileClient:
            return FakeFileClient()

        monkeypatch.setattr(backend, '_file_client', fake_file_client)
        with backend.open(location, encoding='utf-8') as handle:
            assert handle.read() == '{"ok": true}'

    def test_open_writes_binary_payload(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that ABFS writes upload buffered payloads on close."""
        backend = AbfsStorageBackend()
        location = StorageLocation.from_value(
            'abfs://filesystem@example.dfs.core.windows.net/blob.bin',
        )
        uploads: list[dict[str, object]] = []

        class FakeFileClient:
            """Data Lake file client upload test double."""

            def upload_data(self, **kwargs: object) -> None:
                """Record upload arguments."""
                uploads.append(kwargs)

        def fake_file_client(_location: StorageLocation) -> FakeFileClient:
            return FakeFileClient()

        monkeypatch.setattr(backend, '_file_client', fake_file_client)
        monkeypatch.setattr(
            abfs_mod,
            '_import_datalake_types',
            lambda: (object, None),
        )

        with backend.open(location, 'wb') as handle:
            handle.write(b'payload')

        assert uploads == [{'data': b'payload', 'overwrite': True}]

    def test_service_client_uses_connection_string_env(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that ABFS resolves client config from env settings."""
        backend = AbfsStorageBackend()
        location = StorageLocation.from_value(
            'abfs://filesystem@example.dfs.core.windows.net/blob.parquet',
        )
        calls: list[str] = []

        class FakeFileClient:
            """Data Lake file client existence test double."""

            def exists(self) -> bool:
                """Return a present-file result."""
                return True

        class FakeServiceClient:
            """Data Lake service client test double."""

            @classmethod
            def from_connection_string(cls, value: str) -> object:
                """Return a configured service client instance."""
                calls.append(value)
                return cls()

            def get_file_client(self, **kwargs: object) -> FakeFileClient:
                """Return a file client for the requested location."""
                assert kwargs == {
                    'file_path': 'blob.parquet',
                    'file_system': 'filesystem',
                }
                return FakeFileClient()

        monkeypatch.setenv(
            'AZURE_STORAGE_CONNECTION_STRING',
            'UseDevelopmentStorage=true',
        )
        monkeypatch.setattr(
            abfs_mod,
            '_import_datalake_types',
            lambda: (FakeServiceClient, None),
        )

        assert backend.exists(location) is True
        assert calls == ['UseDevelopmentStorage=true']


class TestAzureBlobStorageBackend:
    """Unit tests for :class:`etlplus.storage.AzureBlobStorageBackend`."""

    def test_exists_uses_blob_client(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that Azure Blob exists delegates to the blob client."""
        backend = AzureBlobStorageBackend()
        location = StorageLocation.from_value(
            'azure-blob://container/blob.json',
        )

        class FakeBlobClient:
            """Blob client test double."""

            def exists(self) -> bool:
                """Return a present-object result."""
                return True

        def fake_blob_client(_location: StorageLocation) -> FakeBlobClient:
            return FakeBlobClient()

        monkeypatch.setattr(backend, '_blob_client', fake_blob_client)
        assert backend.exists(location) is True

    def test_exists_raises_import_error_without_sdk(self) -> None:
        """Test that Azure Blob runtime needs the optional SDK package."""
        backend = AzureBlobStorageBackend()
        location = StorageLocation.from_value(
            'azure-blob://container/path/to/blob.json',
        )
        with pytest.raises(
            ImportError,
            match='azure-storage-blob',
        ):
            backend.exists(location)

    def test_ensure_parent_dir_validates_container_and_blob(self) -> None:
        """Test that invalid Azure Blob locations fail validation early."""
        backend = AzureBlobStorageBackend()
        location = StorageLocation.from_value('azure-blob://container')
        with pytest.raises(ValueError, match='blob path'):
            backend.ensure_parent_dir(location)

    def test_inherits_remote_storage_backend_base(self) -> None:
        """Test that Azure Blob uses the shared remote backend base class."""
        assert issubclass(AzureBlobStorageBackend, RemoteStorageBackend)
        assert not issubclass(AzureBlobStorageBackend, StubStorageBackend)

    def test_open_reads_text_payload(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that Azure Blob reads return text buffers when requested."""
        backend = AzureBlobStorageBackend()
        location = StorageLocation.from_value(
            'azure-blob://container/blob.json',
        )

        class FakeDownload:
            """Blob download test double."""

            def readall(self) -> bytes:
                """Return a fixed payload."""
                return b'{"ok": true}'

        class FakeBlobClient:
            """Blob client test double."""

            def download_blob(self) -> FakeDownload:
                """Return the fake download wrapper."""
                return FakeDownload()

        def fake_blob_client(_location: StorageLocation) -> FakeBlobClient:
            return FakeBlobClient()

        monkeypatch.setattr(backend, '_blob_client', fake_blob_client)
        with backend.open(location, encoding='utf-8') as handle:
            assert handle.read() == '{"ok": true}'

    def test_open_writes_binary_payload(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that Azure Blob writes upload buffered payloads on close."""
        backend = AzureBlobStorageBackend()
        location = StorageLocation.from_value(
            'azure-blob://container/blob.bin',
        )
        uploads: list[dict[str, object]] = []

        class FakeBlobClient:
            """Blob client upload test double."""

            def upload_blob(self, **kwargs: object) -> None:
                """Record upload arguments."""
                uploads.append(kwargs)

        def fake_blob_client(_location: StorageLocation) -> FakeBlobClient:
            return FakeBlobClient()

        monkeypatch.setattr(backend, '_blob_client', fake_blob_client)
        monkeypatch.setattr(
            azure_blob_mod,
            '_import_blob_types',
            lambda: (object, None),
        )

        with backend.open(location, 'wb') as handle:
            handle.write(b'payload')

        assert uploads == [{'data': b'payload', 'overwrite': True}]

    def test_service_client_uses_connection_string_env(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that Azure Blob resolves client config from env settings."""
        backend = AzureBlobStorageBackend()
        location = StorageLocation.from_value('azure-blob://container/blob.json')
        calls: list[str] = []

        class FakeBlobClient:
            """Blob client existence test double."""

            def exists(self) -> bool:
                """Return a present-object result."""
                return True

        class FakeBlobServiceClient:
            """Blob service client test double."""

            @classmethod
            def from_connection_string(cls, value: str) -> object:
                """Return a configured service client instance."""
                calls.append(value)
                return cls()

            def get_blob_client(self, **kwargs: object) -> FakeBlobClient:
                """Return a blob client for the requested location."""
                assert kwargs == {
                    'blob': 'blob.json',
                    'container': 'container',
                }
                return FakeBlobClient()

        monkeypatch.setenv(
            'AZURE_STORAGE_CONNECTION_STRING',
            'UseDevelopmentStorage=true',
        )
        monkeypatch.setattr(
            azure_blob_mod,
            '_import_blob_types',
            lambda: (FakeBlobServiceClient, None),
        )

        assert backend.exists(location) is True
        assert calls == ['UseDevelopmentStorage=true']


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

    def test_exists_returns_true_when_object_is_found(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that S3 existence checks return true for present objects."""
        backend = S3StorageBackend()
        location = StorageLocation.from_value('s3://bucket/data.json')

        class FakeS3Client:
            """S3 client existence test double."""

            def head_object(self, **kwargs: object) -> None:
                """Assert the requested object identity."""
                assert kwargs == {'Bucket': 'bucket', 'Key': 'data.json'}

        def fake_client() -> FakeS3Client:
            return FakeS3Client()

        monkeypatch.setattr(backend, '_client', fake_client)
        assert backend.exists(location) is True

    def test_inherits_remote_storage_backend_base(self) -> None:
        """Test that S3 uses the shared remote backend base class."""
        assert issubclass(S3StorageBackend, RemoteStorageBackend)
        assert not issubclass(S3StorageBackend, StubStorageBackend)

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
        with pytest.raises(ImportError, match='boto3'):
            backend.exists(location)

    def test_exists_returns_false_for_missing_object(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that S3 existence checks treat not-found errors as false."""
        backend = S3StorageBackend()
        location = StorageLocation.from_value('s3://bucket/missing.json')

        class FakeS3Error(Exception):
            """S3 not-found error test double."""

            def __init__(self) -> None:
                self.response = {'Error': {'Code': 'NoSuchKey'}}

        class FakeS3Client:
            """S3 client missing-object test double."""

            def head_object(self, **kwargs: object) -> None:
                """Raise a not-found error for the requested object."""
                raise FakeS3Error()

        def fake_client() -> FakeS3Client:
            return FakeS3Client()

        monkeypatch.setattr(backend, '_client', fake_client)
        assert backend.exists(location) is False

    def test_open_reads_text_payload(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that S3 reads return text buffers when requested."""
        backend = S3StorageBackend()
        location = StorageLocation.from_value('s3://bucket/data.json')

        class FakeS3Client:
            """S3 client read test double."""

            def get_object(self, **kwargs: object) -> dict[str, object]:
                """Return the requested object payload."""
                assert kwargs == {'Bucket': 'bucket', 'Key': 'data.json'}
                return {'Body': BytesIO(b'{"ok": true}')}

        def fake_client() -> FakeS3Client:
            return FakeS3Client()

        monkeypatch.setattr(backend, '_client', fake_client)
        with backend.open(location, encoding='utf-8') as handle:
            assert handle.read() == '{"ok": true}'

    def test_open_writes_binary_payload(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that S3 writes upload buffered payloads on close."""
        backend = S3StorageBackend()
        location = StorageLocation.from_value('s3://bucket/data.bin')
        uploads: list[dict[str, object]] = []

        class FakeS3Client:
            """S3 client write test double."""

            def put_object(self, **kwargs: object) -> None:
                """Record upload arguments."""
                uploads.append(kwargs)

        def fake_client() -> FakeS3Client:
            return FakeS3Client()

        monkeypatch.setattr(backend, '_client', fake_client)
        with backend.open(location, 'wb') as handle:
            handle.write(b'payload')

        assert uploads == [
            {
                'Body': b'payload',
                'Bucket': 'bucket',
                'Key': 'data.bin',
            },
        ]


class TestStorageLocation:
    """Unit tests for :class:`etlplus.storage.StorageLocation`."""

    def test_from_abfs_uri(self) -> None:
        """Test that ABFS URIs keep authority and filesystem path segments."""
        location = StorageLocation.from_value(
            ('abfs://filesystem@example.dfs.core.windows.net/path/to/blob.parquet'),
        )
        assert location.scheme is StorageScheme.ABFS
        assert location.authority == 'filesystem@example.dfs.core.windows.net'
        assert location.path == 'path/to/blob.parquet'

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

    def test_get_backend_for_abfs_location(self) -> None:
        """Test that ABFS locations resolve to the ABFS backend."""
        backend = get_backend(
            'abfs://filesystem@example.dfs.core.windows.net/path.json',
        )
        assert isinstance(backend, AbfsStorageBackend)

    def test_get_backend_for_azure_blob_location(self) -> None:
        """Test that Azure Blob locations resolve to the Azure backend."""
        backend = get_backend('azure-blob://container/path.json')
        assert isinstance(backend, AzureBlobStorageBackend)

    def test_get_backend_for_ftp_location(self) -> None:
        """Test that FTP locations resolve to the FTP backend stub."""
        backend = get_backend('ftp://example.com/path.json')
        assert isinstance(backend, FtpStorageBackend)

    def test_get_backend_for_local_location(self) -> None:
        """Test that local storage resolves to the local backend."""
        backend = get_backend('data/input.csv')
        assert isinstance(backend, LocalStorageBackend)

    def test_get_backend_for_s3_location(self) -> None:
        """Test that S3 locations resolve to the S3 backend skeleton."""
        backend = get_backend('s3://bucket/path.json')
        assert isinstance(backend, S3StorageBackend)


class TestOtherStubStorageBackends:
    """Unit tests for other placeholder storage backends."""

    def test_ftp_exists_raises_placeholder_error(self) -> None:
        """Test that FTP routes through the shared placeholder behavior."""
        backend = FtpStorageBackend()
        location = StorageLocation.from_value('ftp://example.com/data.json')
        with pytest.raises(NotImplementedError, match='ftplib'):
            backend.exists(location)
