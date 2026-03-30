"""
:mod:`tests.unit.storage.test_u_storage` module.

Unit tests for :mod:`etlplus.storage`.
"""

from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
from io import TextIOWrapper
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from etlplus.storage import AbfsStorageBackend
from etlplus.storage import AzureBlobStorageBackend
from etlplus.storage import FtpStorageBackend
from etlplus.storage import HttpStorageBackend
from etlplus.storage import LocalStorageBackend
from etlplus.storage import RemoteStorageBackend
from etlplus.storage import S3StorageBackend
from etlplus.storage import StorageLocation
from etlplus.storage import StorageScheme
from etlplus.storage import StubStorageBackend
from etlplus.storage import _abfs as abfs_mod
from etlplus.storage import _azure_blob as azure_blob_mod
from etlplus.storage import _http as http_mod
from etlplus.storage import _registry as registry_mod
from etlplus.storage import _remote_buffer as remote_buffer_mod
from etlplus.storage import _s3 as s3_mod
from etlplus.storage import coerce_location
from etlplus.storage import get_backend

# SECTION: HELPERS ========================================================== #


class FakeHttpResponse:
    """Minimal HTTP response test double."""

    # -- Magic Methods (Object Lifecycle) -- #

    def __init__(
        self,
        *,
        status_code: int,
        payload: bytes = b'',
    ) -> None:
        self.status_code = status_code
        self.content = payload

    # -- Instance Methods -- #

    def close(self) -> None:
        """Close the response without side effects."""

    def raise_for_status(self) -> None:
        """Raise one error for non-successful response codes."""
        if self.status_code >= 400:
            raise RuntimeError(f'HTTP {self.status_code}')


class FakeHttpSession:
    """Minimal requests-session test double for HTTP storage tests."""

    # -- Magic Methods (Object Lifecycle) -- #

    def __init__(
        self,
        *,
        head_status: int = 200,
        get_status: int = 200,
        payload: bytes = b'',
    ) -> None:
        self.calls: list[tuple[str, str, bool]] = []
        self.head_status = head_status
        self.get_status = get_status
        self.payload = payload

    # -- Instance Methods -- #

    def close(self) -> None:
        """Close the fake session without side effects."""

    def get(self, url: str, **kwargs: Any) -> FakeHttpResponse:
        """Return one fake GET response and capture call metadata."""
        self.calls.append(('get', url, bool(kwargs.get('stream', False))))
        return FakeHttpResponse(
            status_code=self.get_status,
            payload=self.payload,
        )

    def head(self, url: str, **kwargs: Any) -> FakeHttpResponse:
        """Return one fake HEAD response and capture call metadata."""
        self.calls.append(
            ('head', url, bool(kwargs.get('allow_redirects', False))),
        )
        return FakeHttpResponse(status_code=self.head_status)


@dataclass(slots=True)
class FakeContentSettings:
    """Minimal content-settings test double."""

    content_type: str


# SECTION: TESTS ============================================================ #


class TestAbfsStorageBackend:
    """Unit tests for :class:`etlplus.storage.AbfsStorageBackend`."""

    def test_import_datalake_types_returns_sdk_types(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that SDK types are returned from the Azure import helper."""

        class FakeModule:
            DataLakeServiceClient = object()
            ContentSettings = object()

        monkeypatch.setattr(abfs_mod, 'import_module', lambda _: FakeModule)

        assert abfs_mod._import_datalake_types() == (
            FakeModule.DataLakeServiceClient,
            FakeModule.ContentSettings,
        )

    def test_account_url_from_authority_uses_account_host(self) -> None:
        """Test that ABFS account URLs are derived from the authority host."""
        backend = AbfsStorageBackend()
        assert (
            backend._account_url_from_authority(
                'filesystem@example.dfs.core.windows.net',
            )
            == 'https://example.dfs.core.windows.net'
        )

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

    def test_delete_uses_file_client(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that ABFS delete delegates to the file client."""
        backend = AbfsStorageBackend()
        location = StorageLocation.from_value(
            'abfs://filesystem@example.dfs.core.windows.net/blob.json',
        )
        deleted: list[bool] = []

        class FakeFileClient:
            """Data Lake file client delete test double."""

            def delete_file(self) -> None:
                """Record that delete_file was invoked."""
                deleted.append(True)

        def fake_file_client(_location: StorageLocation) -> FakeFileClient:
            return FakeFileClient()

        monkeypatch.setattr(backend, '_file_client', fake_file_client)
        backend.delete(location)
        assert deleted == [True]

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

    def test_open_rejects_unexpected_kwargs(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that ABFS open rejects unsupported keyword arguments."""
        backend = AbfsStorageBackend()
        location = StorageLocation.from_value(
            'abfs://filesystem@example.dfs.core.windows.net/blob.bin',
        )
        monkeypatch.setattr(backend, '_file_client', lambda _location: object())

        with pytest.raises(TypeError, match='Unsupported ABFS open'):
            backend.open(location, 'rb', unsupported=True)

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

    def test_open_writes_content_settings_when_available(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that ABFS uploads include content settings when requested."""
        backend = AbfsStorageBackend()
        location = StorageLocation.from_value(
            'abfs://filesystem@example.dfs.core.windows.net/blob.bin',
        )
        uploads: list[dict[str, object]] = []

        class FakeFileClient:
            def upload_data(self, **kwargs: object) -> None:
                uploads.append(kwargs)

        monkeypatch.setattr(
            backend,
            '_file_client',
            lambda _location: FakeFileClient(),
        )
        monkeypatch.setattr(
            abfs_mod,
            '_import_datalake_types',
            lambda: (object, FakeContentSettings),
        )

        with backend.open(
            location,
            'wb',
            content_type='application/octet-stream',
        ) as handle:
            handle.write(b'payload')

        assert uploads[0]['data'] == b'payload'
        assert uploads[0]['overwrite'] is True
        assert isinstance(uploads[0]['content_settings'], FakeContentSettings)
        assert uploads[0]['content_settings'].content_type == 'application/octet-stream'

    def test_service_client_derives_account_url_from_location(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that ABFS builds a service client from the location authority."""
        backend = AbfsStorageBackend()
        location = StorageLocation.from_value(
            'abfs://filesystem@example.dfs.core.windows.net/blob.parquet',
        )
        calls: list[dict[str, object]] = []

        class FakeServiceClient:
            def __init__(
                self,
                *,
                account_url: str,
                credential: object | None = None,
            ) -> None:
                calls.append(
                    {
                        'account_url': account_url,
                        'credential': credential,
                    },
                )

        monkeypatch.delenv('AZURE_STORAGE_CONNECTION_STRING', raising=False)
        monkeypatch.delenv('AZURE_STORAGE_ACCOUNT_URL', raising=False)
        monkeypatch.delenv('AZURE_STORAGE_CREDENTIAL', raising=False)
        monkeypatch.setattr(
            abfs_mod,
            '_import_datalake_types',
            lambda: (FakeServiceClient, None),
        )

        backend._service_client(location)

        assert calls == [
            {
                'account_url': 'https://example.dfs.core.windows.net',
                'credential': None,
            },
        ]

    def test_service_client_requires_resolvable_account_url(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that ABFS rejects missing connection and account settings."""
        backend = AbfsStorageBackend()

        class FakeServiceClient:
            pass

        monkeypatch.delenv('AZURE_STORAGE_CONNECTION_STRING', raising=False)
        monkeypatch.delenv('AZURE_STORAGE_ACCOUNT_URL', raising=False)
        monkeypatch.delenv('AZURE_STORAGE_CREDENTIAL', raising=False)
        monkeypatch.setattr(
            abfs_mod,
            '_import_datalake_types',
            lambda: (FakeServiceClient, None),
        )

        with pytest.raises(ValueError, match='AZURE_STORAGE_CONNECTION_STRING'):
            backend._service_client()

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

    def test_service_client_uses_credential_env(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that ABFS forwards configured credentials when present."""
        backend = AbfsStorageBackend()
        location = StorageLocation.from_value(
            'abfs://filesystem@example.dfs.core.windows.net/blob.parquet',
        )
        calls: list[dict[str, object]] = []

        class FakeServiceClient:
            def __init__(
                self,
                *,
                account_url: str,
                credential: object | None = None,
            ) -> None:
                calls.append(
                    {
                        'account_url': account_url,
                        'credential': credential,
                    },
                )

        monkeypatch.delenv('AZURE_STORAGE_CONNECTION_STRING', raising=False)
        monkeypatch.delenv('AZURE_STORAGE_ACCOUNT_URL', raising=False)
        monkeypatch.setenv('AZURE_STORAGE_CREDENTIAL', 'secret')
        monkeypatch.setattr(
            abfs_mod,
            '_import_datalake_types',
            lambda: (FakeServiceClient, None),
        )

        backend._service_client(location)

        assert calls == [
            {
                'account_url': 'https://example.dfs.core.windows.net',
                'credential': 'secret',
            },
        ]

    def test_service_client_uses_explicit_credential(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that ABFS preserves an explicit constructor credential."""
        credential = object()
        backend = AbfsStorageBackend(credential=credential)
        location = StorageLocation.from_value(
            'abfs://filesystem@example.dfs.core.windows.net/blob.parquet',
        )
        calls: list[dict[str, object]] = []

        class FakeServiceClient:
            def __init__(
                self,
                *,
                account_url: str,
                credential: object | None = None,
            ) -> None:
                calls.append(
                    {
                        'account_url': account_url,
                        'credential': credential,
                    },
                )

        monkeypatch.delenv('AZURE_STORAGE_CONNECTION_STRING', raising=False)
        monkeypatch.delenv('AZURE_STORAGE_ACCOUNT_URL', raising=False)
        monkeypatch.delenv('AZURE_STORAGE_CREDENTIAL', raising=False)
        monkeypatch.setattr(
            abfs_mod,
            '_import_datalake_types',
            lambda: (FakeServiceClient, None),
        )

        backend._service_client(location)

        assert calls == [
            {
                'account_url': 'https://example.dfs.core.windows.net',
                'credential': credential,
            },
        ]

    @pytest.mark.parametrize(
        'authority',
        [
            'example.dfs.core.windows.net',
            '@example.dfs.core.windows.net',
            'filesystem@',
        ],
    )
    def test_split_authority_rejects_invalid_values(self, authority: str) -> None:
        """Test that malformed ABFS authorities are rejected."""
        backend = AbfsStorageBackend()
        with pytest.raises(ValueError, match='filesystem@account'):
            backend._split_authority(authority)


class TestAzureBlobStorageBackend:
    """Unit tests for :class:`etlplus.storage.AzureBlobStorageBackend`."""

    def test_account_url_from_authority_returns_none_without_account_host(self) -> None:
        """Test that simple container authorities do not imply an account URL."""
        backend = AzureBlobStorageBackend()
        assert backend._account_url_from_authority('container') is None

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

    def test_delete_uses_blob_client(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that Azure Blob delete delegates to the blob client."""
        backend = AzureBlobStorageBackend()
        location = StorageLocation.from_value('azure-blob://container/blob.json')
        deletes: list[object] = []

        class FakeBlobClient:
            """Blob client delete test double."""

            def delete_blob(self, **kwargs: object) -> None:
                """Record blob deletion arguments."""
                deletes.append(kwargs)

        def fake_blob_client(_location: StorageLocation) -> FakeBlobClient:
            return FakeBlobClient()

        monkeypatch.setattr(backend, '_blob_client', fake_blob_client)
        backend.delete(location)
        assert deletes == [{'delete_snapshots': 'include'}]

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

    def test_import_blob_types_returns_sdk_types(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that SDK types are returned from the Azure Blob import helper."""

        class FakeModule:
            BlobServiceClient = object()
            ContentSettings = object()

        monkeypatch.setattr(azure_blob_mod, 'import_module', lambda _: FakeModule)

        assert azure_blob_mod._import_blob_types() == (
            FakeModule.BlobServiceClient,
            FakeModule.ContentSettings,
        )

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

    def test_open_writes_content_settings_when_available(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that Azure Blob uploads include content settings when requested."""
        backend = AzureBlobStorageBackend()
        location = StorageLocation.from_value('azure-blob://container/blob.bin')
        uploads: list[dict[str, object]] = []

        class FakeBlobClient:
            def upload_blob(self, **kwargs: object) -> None:
                uploads.append(kwargs)

        monkeypatch.setattr(
            backend,
            '_blob_client',
            lambda _location: FakeBlobClient(),
        )
        monkeypatch.setattr(
            azure_blob_mod,
            '_import_blob_types',
            lambda: (object, FakeContentSettings),
        )

        with backend.open(location, 'wb', content_type='application/json') as handle:
            handle.write(b'payload')

        assert uploads[0]['data'] == b'payload'
        assert uploads[0]['overwrite'] is True
        assert isinstance(uploads[0]['content_settings'], FakeContentSettings)
        assert uploads[0]['content_settings'].content_type == 'application/json'

    def test_service_client_derives_account_url_from_https_authority(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that HTTPS Azure Blob URLs provide the account host."""
        backend = AzureBlobStorageBackend()
        location = StorageLocation.from_value(
            'https://example.blob.core.windows.net/container/blob.json',
        )
        calls: list[str] = []

        class FakeBlobClient:
            """Blob client existence test double."""

            def exists(self) -> bool:
                """Return a present-object result."""
                return True

        class FakeBlobServiceClient:
            """Blob service client test double."""

            def __init__(
                self,
                *,
                account_url: str,
                credential: object | None = None,
            ) -> None:
                assert credential is None
                calls.append(account_url)

            def get_blob_client(self, **kwargs: object) -> FakeBlobClient:
                """Return a blob client for the requested location."""
                assert kwargs == {
                    'blob': 'blob.json',
                    'container': 'container',
                }
                return FakeBlobClient()

        monkeypatch.delenv('AZURE_STORAGE_CONNECTION_STRING', raising=False)
        monkeypatch.delenv('AZURE_STORAGE_ACCOUNT_URL', raising=False)
        monkeypatch.delenv('AZURE_STORAGE_CREDENTIAL', raising=False)
        monkeypatch.setattr(
            azure_blob_mod,
            '_import_blob_types',
            lambda: (FakeBlobServiceClient, None),
        )

        assert backend.exists(location) is True
        assert calls == ['https://example.blob.core.windows.net']

    def test_service_client_requires_resolvable_account_url(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that Azure Blob rejects missing connection and account settings."""
        backend = AzureBlobStorageBackend()

        class FakeBlobServiceClient:
            pass

        monkeypatch.delenv('AZURE_STORAGE_CONNECTION_STRING', raising=False)
        monkeypatch.delenv('AZURE_STORAGE_ACCOUNT_URL', raising=False)
        monkeypatch.delenv('AZURE_STORAGE_CREDENTIAL', raising=False)
        monkeypatch.setattr(
            azure_blob_mod,
            '_import_blob_types',
            lambda: (FakeBlobServiceClient, None),
        )

        with pytest.raises(ValueError, match='AZURE_STORAGE_CONNECTION_STRING'):
            backend._service_client()

    def test_service_client_uses_credential_env(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that Azure Blob forwards configured credentials when present."""
        backend = AzureBlobStorageBackend()
        location = StorageLocation.from_value(
            'azure-blob://container@example.blob.core.windows.net/blob.json',
        )
        calls: list[dict[str, object]] = []

        class FakeBlobServiceClient:
            def __init__(
                self,
                *,
                account_url: str,
                credential: object | None = None,
            ) -> None:
                calls.append(
                    {
                        'account_url': account_url,
                        'credential': credential,
                    },
                )

        monkeypatch.delenv('AZURE_STORAGE_CONNECTION_STRING', raising=False)
        monkeypatch.delenv('AZURE_STORAGE_ACCOUNT_URL', raising=False)
        monkeypatch.setenv('AZURE_STORAGE_CREDENTIAL', 'secret')
        monkeypatch.setattr(
            azure_blob_mod,
            '_import_blob_types',
            lambda: (FakeBlobServiceClient, None),
        )

        backend._service_client(location)

        assert calls == [
            {
                'account_url': 'https://example.blob.core.windows.net',
                'credential': 'secret',
            },
        ]

    def test_service_client_uses_explicit_credential(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that Azure Blob preserves an explicit constructor credential."""
        credential = object()
        backend = AzureBlobStorageBackend(credential=credential)
        location = StorageLocation.from_value(
            'azure-blob://container@example.blob.core.windows.net/blob.json',
        )
        calls: list[dict[str, object]] = []

        class FakeBlobServiceClient:
            def __init__(
                self,
                *,
                account_url: str,
                credential: object | None = None,
            ) -> None:
                calls.append(
                    {
                        'account_url': account_url,
                        'credential': credential,
                    },
                )

        monkeypatch.delenv('AZURE_STORAGE_CONNECTION_STRING', raising=False)
        monkeypatch.delenv('AZURE_STORAGE_ACCOUNT_URL', raising=False)
        monkeypatch.delenv('AZURE_STORAGE_CREDENTIAL', raising=False)
        monkeypatch.setattr(
            azure_blob_mod,
            '_import_blob_types',
            lambda: (FakeBlobServiceClient, None),
        )

        backend._service_client(location)

        assert calls == [
            {
                'account_url': 'https://example.blob.core.windows.net',
                'credential': credential,
            },
        ]

    @pytest.mark.parametrize(
        'authority',
        [
            '',
            'container@',
        ],
    )
    def test_split_authority_rejects_invalid_values(self, authority: str) -> None:
        """Test that malformed Azure Blob authorities are rejected."""
        backend = AzureBlobStorageBackend()
        with pytest.raises(ValueError, match='container'):
            backend._split_authority(authority)

    def test_open_rejects_unexpected_kwargs(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that Azure Blob open rejects unsupported keyword arguments."""
        backend = AzureBlobStorageBackend()
        location = StorageLocation.from_value('azure-blob://container/blob.bin')
        monkeypatch.setattr(backend, '_blob_client', lambda _location: object())

        with pytest.raises(TypeError, match='Unsupported Azure Blob open'):
            backend.open(location, 'rb', unsupported=True)


class TestHttpStorageBackend:
    """Unit tests for :class:`etlplus.storage.HttpStorageBackend`."""

    def test_delete_rejects_cleanup(self) -> None:
        """Test that HTTP backend explicitly rejects deletion."""
        backend = HttpStorageBackend(session=FakeHttpSession(get_status=200))
        location = StorageLocation.from_value('https://example.com/files/data.csv')

        with pytest.raises(ValueError, match='read-only'):
            backend.delete(location)

    def test_exists_returns_false_for_not_found(self) -> None:
        """Test that HTTP exists returns false for 404 responses."""
        backend = HttpStorageBackend(session=FakeHttpSession(head_status=404))
        location = StorageLocation.from_value('https://example.com/files/data.csv')

        assert backend.exists(location) is False

    def test_exists_returns_true_for_successful_head(self) -> None:
        """Test that HTTP exists returns true for successful HEAD calls."""
        session = FakeHttpSession(head_status=200)
        backend = HttpStorageBackend(session=session)
        location = StorageLocation.from_value('https://example.com/files/data.csv')

        assert backend.exists(location) is True
        assert session.calls == [
            ('head', 'https://example.com/files/data.csv', True),
        ]

    def test_exists_falls_back_to_get_when_head_not_supported(self) -> None:
        """Test that HTTP exists falls back to GET when HEAD is unsupported."""
        session = FakeHttpSession(head_status=405, get_status=200)
        backend = HttpStorageBackend(session=session)
        location = StorageLocation.from_value('https://example.com/files/data.csv')

        assert backend.exists(location) is True
        assert session.calls == [
            ('head', 'https://example.com/files/data.csv', True),
            ('get', 'https://example.com/files/data.csv', True),
        ]

    def test_exists_returns_false_when_get_fallback_is_not_found(self) -> None:
        """Test that HTTP exists treats a 404 fallback GET as missing."""
        session = FakeHttpSession(head_status=405, get_status=404)
        backend = HttpStorageBackend(session=session)
        location = StorageLocation.from_value('https://example.com/files/data.csv')

        assert backend.exists(location) is False

    def test_open_raises_file_not_found_for_missing_resource(self) -> None:
        """Test that HTTP open maps 404 responses to FileNotFoundError."""
        backend = HttpStorageBackend(session=FakeHttpSession(get_status=404))
        location = StorageLocation.from_value('https://example.com/files/data.csv')

        with pytest.raises(FileNotFoundError, match='File not found'):
            backend.open(location)

    def test_open_reads_text_payload(self) -> None:
        """Test that HTTP open returns a readable text buffer."""
        backend = HttpStorageBackend(
            session=FakeHttpSession(get_status=200, payload=b'name\nAda\n'),
        )
        location = StorageLocation.from_value('https://example.com/files/data.csv')

        with backend.open(location, encoding='utf-8') as handle:
            assert handle.read() == 'name\nAda\n'

    def test_open_rejects_unexpected_kwargs(self) -> None:
        """Test that HTTP open rejects unsupported keyword arguments."""
        backend = HttpStorageBackend(session=FakeHttpSession(get_status=200))
        location = StorageLocation.from_value('https://example.com/files/data.csv')

        with pytest.raises(TypeError, match='Unsupported HTTP open'):
            backend.open(location, unsupported=True)

    @pytest.mark.parametrize('mode', ['w', 'wb', 'wt'])
    def test_open_rejects_write_modes(self, mode: str) -> None:
        """Test that HTTP backend is explicitly read-only."""
        backend = HttpStorageBackend(session=FakeHttpSession(get_status=200))
        location = StorageLocation.from_value('https://example.com/files/data.csv')

        with pytest.raises(ValueError, match='read-only'):
            backend.open(location, mode)

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

    def test_session_scope_closes_owned_session(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that owned sessions are closed when the scope exits."""
        closed: list[bool] = []

        class FakeOwnedSession(FakeHttpSession):
            def close(self) -> None:
                closed.append(True)

        session = FakeOwnedSession()
        monkeypatch.setattr(http_mod.requests, 'Session', lambda: session)
        backend = HttpStorageBackend()

        with backend._session_scope() as scoped_session:
            assert scoped_session is session

        assert closed == [True]


class TestLocalStorageBackend:
    """Unit tests for :class:`etlplus.storage.LocalStorageBackend`."""

    def test_open_creates_parent_for_write_modes(self, tmp_path: Path) -> None:
        """Test that write modes create missing parent directories."""
        target = tmp_path / 'nested' / 'output.txt'
        backend = LocalStorageBackend()
        location = StorageLocation.from_value(target)

        with backend.open(location, 'w', encoding='utf-8') as handle:
            handle.write('payload')

        assert target.read_text(encoding='utf-8') == 'payload'

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

        assert created == []


class TestOtherStubStorageBackends:
    """Unit tests for other placeholder storage backends."""

    def test_ftp_exists_raises_placeholder_error(self) -> None:
        """Test that FTP routes through the shared placeholder behavior."""
        backend = FtpStorageBackend()
        location = StorageLocation.from_value('ftp://example.com/data.json')
        with pytest.raises(NotImplementedError, match='ftplib'):
            backend.exists(location)

    def test_ftp_delete_raises_placeholder_error(self) -> None:
        """Test that FTP delete routes through the shared placeholder behavior."""
        backend = FtpStorageBackend()
        location = StorageLocation.from_value('ftp://example.com/data.json')
        with pytest.raises(NotImplementedError, match='ftplib'):
            backend.delete(location)

    def test_ftp_open_raises_placeholder_error(self) -> None:
        """Test that FTP open routes through the shared placeholder behavior."""
        backend = FtpStorageBackend()
        location = StorageLocation.from_value('ftp://example.com/data.json')
        with pytest.raises(NotImplementedError, match='ftplib'):
            backend.open(location, 'rb', newline=None)


class TestRemoteBufferHelpers:
    """Unit tests for the shared in-memory remote buffer helpers."""

    def test_read_buffer_binary_mode_returns_bytes_buffer(self) -> None:
        """Test that binary read mode returns the raw bytes buffer."""
        handle = remote_buffer_mod.open_remote_buffer(
            kind='read',
            text_mode=False,
            payload=b'payload',
        )

        assert isinstance(handle, BytesIO)
        assert handle.read() == b'payload'

    def test_read_buffer_requires_payload(self) -> None:
        """Test that read buffers require a payload."""
        with pytest.raises(ValueError, match='payload is required'):
            remote_buffer_mod.open_remote_buffer(kind='read', text_mode=False)

    def test_upload_buffer_uploads_only_once_on_double_close(self) -> None:
        """Test that upload-on-close buffers do not upload twice."""
        uploads: list[bytes] = []
        handle = remote_buffer_mod.open_remote_buffer(
            kind='write',
            text_mode=False,
            uploader=uploads.append,
        )

        handle.write(b'payload')
        handle.close()
        handle.close()

        assert uploads == [b'payload']

    def test_write_buffer_requires_uploader(self) -> None:
        """Test that write buffers require an upload callback."""
        with pytest.raises(ValueError, match='uploader is required'):
            remote_buffer_mod.open_remote_buffer(kind='write', text_mode=False)

    def test_write_buffer_text_mode_returns_text_wrapper(self) -> None:
        """Test that text write mode wraps the upload buffer in text I/O."""
        uploads: list[bytes] = []
        handle = remote_buffer_mod.open_remote_buffer(
            kind='write',
            text_mode=True,
            uploader=uploads.append,
        )

        assert isinstance(handle, TextIOWrapper)
        handle.write('payload')
        handle.close()
        assert uploads == [b'payload']

    def test_parse_remote_open_mode_rejects_invalid_mode(self) -> None:
        """Test that invalid remote open modes are rejected."""
        with pytest.raises(ValueError, match='support only'):
            remote_buffer_mod.parse_remote_open_mode('a')


class TestRemoteStorageBackend:
    """Unit tests for shared remote-backend validation."""

    def test_validate_rejects_wrong_scheme(self) -> None:
        """Test that remote backends reject locations with the wrong scheme."""
        backend = S3StorageBackend()
        location = StorageLocation.from_value('https://example.com/files/data.csv')

        with pytest.raises(TypeError, match='only supports'):
            backend.ensure_parent_dir(location)

    def test_validate_requires_authority(self) -> None:
        """Test that remote backends reject locations without authority."""
        backend = S3StorageBackend()
        location = StorageLocation(
            raw='s3:///data.csv',
            scheme=StorageScheme.S3,
            authority='',
            path='data.csv',
        )

        with pytest.raises(ValueError, match='bucket name'):
            backend.ensure_parent_dir(location)


class TestS3StorageBackend:
    """Unit tests for :class:`etlplus.storage.S3StorageBackend`."""

    def test_client_uses_imported_boto3_factory(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that the S3 backend resolves the boto3 client factory."""
        marker = object()

        class FakeBoto3:
            def client(self, service: str) -> object:
                assert service == 's3'
                return marker

        backend = S3StorageBackend()
        monkeypatch.setattr(s3_mod, '_import_boto3', lambda: FakeBoto3())

        assert backend._client() is marker

    def test_delete_uses_client(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that S3 delete delegates to the client."""
        backend = S3StorageBackend()
        location = StorageLocation.from_value('s3://bucket/data.json')
        deletes: list[dict[str, object]] = []

        class FakeS3Client:
            """S3 client delete test double."""

            def delete_object(self, **kwargs: object) -> None:
                """Record object deletion arguments."""
                deletes.append(kwargs)

        def fake_client() -> FakeS3Client:
            return FakeS3Client()

        monkeypatch.setattr(backend, '_client', fake_client)
        backend.delete(location)
        assert deletes == [{'Bucket': 'bucket', 'Key': 'data.json'}]

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

    def test_exists_reraises_non_not_found_errors(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that S3 existence checks propagate non-not-found errors."""
        backend = S3StorageBackend()
        location = StorageLocation.from_value('s3://bucket/data.json')

        class FakeS3Error(Exception):
            def __init__(self) -> None:
                self.response = {'Error': {'Code': 'AccessDenied'}}

        class FakeS3Client:
            def head_object(self, **kwargs: object) -> None:
                raise FakeS3Error()

        monkeypatch.setattr(backend, '_client', lambda: FakeS3Client())

        with pytest.raises(FakeS3Error):
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

    def test_is_not_found_error_returns_false_without_mapping_response(self) -> None:
        """Test that malformed boto-style errors are not treated as missing."""
        backend = S3StorageBackend()

        class FakeS3Error(Exception):
            response = 'not-a-dict'

        assert backend._is_not_found_error(FakeS3Error()) is False

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

    def test_open_rejects_unexpected_kwargs(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that S3 open rejects unsupported keyword arguments."""
        backend = S3StorageBackend()
        location = StorageLocation.from_value('s3://bucket/data.bin')
        monkeypatch.setattr(backend, '_client', lambda: object())

        with pytest.raises(TypeError, match='Unsupported S3 open'):
            backend.open(location, 'rb', unsupported=True)

    def test_open_writes_content_type_when_provided(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that S3 uploads include ContentType when requested."""
        backend = S3StorageBackend()
        location = StorageLocation.from_value('s3://bucket/data.bin')
        uploads: list[dict[str, object]] = []

        class FakeS3Client:
            def put_object(self, **kwargs: object) -> None:
                uploads.append(kwargs)

        monkeypatch.setattr(backend, '_client', lambda: FakeS3Client())

        with backend.open(
            location,
            'wb',
            content_type='application/octet-stream',
        ) as handle:
            handle.write(b'payload')

        assert uploads == [
            {
                'Body': b'payload',
                'Bucket': 'bucket',
                'Key': 'data.bin',
                'ContentType': 'application/octet-stream',
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

    def test_from_https_abfs_url(self) -> None:
        """Test that ADLS HTTPS URLs normalize into ABFS storage locations."""
        location = StorageLocation.from_value(
            'https://example.dfs.core.windows.net/filesystem/path/to/blob.parquet',
        )
        assert location.scheme is StorageScheme.ABFS
        assert location.authority == 'filesystem@example.dfs.core.windows.net'
        assert location.path == 'path/to/blob.parquet'

    def test_from_https_azure_blob_url(self) -> None:
        """Test that Azure Blob HTTPS URLs normalize into blob locations."""
        location = StorageLocation.from_value(
            'https://example.blob.core.windows.net/container/path/to/blob.json',
        )
        assert location.scheme is StorageScheme.AZURE_BLOB
        assert location.authority == 'container@example.blob.core.windows.net'
        assert location.path == 'path/to/blob.json'

    def test_from_https_url(self) -> None:
        """Test that generic HTTPS URLs normalize into HTTP locations."""
        location = StorageLocation.from_value(
            'https://example.com/files/data.csv?download=1',
        )
        assert location.scheme is StorageScheme.HTTP
        assert location.authority == 'example.com'
        assert location.path == 'files/data.csv'

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

    def test_from_remote_uri(self) -> None:
        """Test that remote URIs keep scheme, authority, and relative path."""
        location = StorageLocation.from_value(
            's3://bucket/path/to/object.parquet',
        )
        assert location.scheme is StorageScheme.S3
        assert location.authority == 'bucket'
        assert location.path == 'path/to/object.parquet'
        assert location.is_remote is True

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
        ],
    )
    def test_from_scheme_alias_uri(
        self,
        raw: str,
        scheme: StorageScheme,
    ) -> None:
        """Test that known remote scheme aliases normalize correctly."""
        location = StorageLocation.from_value(raw)
        assert location.scheme is scheme

    def test_from_value_rejects_empty_input(self) -> None:
        """Test that blank location inputs are rejected."""
        with pytest.raises(ValueError, match='cannot be empty'):
            StorageLocation.from_value('  ')

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

    def test_get_backend_for_http_location(self) -> None:
        """Test that HTTP locations resolve to the HTTP backend."""
        backend = get_backend('https://example.com/files/data.csv')
        assert isinstance(backend, HttpStorageBackend)

    def test_get_backend_for_local_location(self) -> None:
        """Test that local storage resolves to the local backend."""
        backend = get_backend('data/input.csv')
        assert isinstance(backend, LocalStorageBackend)

    def test_get_backend_for_s3_location(self) -> None:
        """Test that S3 locations resolve to the S3 backend skeleton."""
        backend = get_backend('s3://bucket/path.json')
        assert isinstance(backend, S3StorageBackend)

    def test_get_backend_rejects_unsupported_scheme(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that unsupported schemes raise a clear error."""
        monkeypatch.setattr(
            registry_mod,
            'coerce_location',
            lambda _value: SimpleNamespace(
                scheme=SimpleNamespace(value='custom'),
            ),
        )

        with pytest.raises(NotImplementedError, match="'custom'"):
            registry_mod.get_backend('custom://target')
