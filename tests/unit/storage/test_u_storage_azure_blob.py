"""
:mod:`tests.unit.storage.test_u_storage_azure_blob` module.

Unit tests for :mod:`etlplus.storage._azure_blob`.
"""

from __future__ import annotations

import pytest

from etlplus.storage import AzureBlobStorageBackend
from etlplus.storage import StorageLocation
from etlplus.storage import _azure_blob as azure_blob_mod

from .pytest_storage_support import FakeContentSettings

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestAzureBlobStorageBackend:
    """Unit tests for :class:`etlplus.storage.AzureBlobStorageBackend`."""

    def test_account_url_from_authority_returns_none_without_account_host(self) -> None:
        """Test that simple container authorities do not imply an account URL."""
        backend = AzureBlobStorageBackend()
        assert backend._account_url_from_authority('container') is None

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

        monkeypatch.setattr(backend, '_blob_client', lambda _location: FakeBlobClient())
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

        monkeypatch.setattr(backend, '_blob_client', lambda _location: FakeBlobClient())
        assert backend.exists(location) is True

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

        monkeypatch.setattr(backend, '_blob_client', lambda _location: FakeBlobClient())
        with backend.open(location, encoding='utf-8') as handle:
            assert handle.read() == '{"ok": true}'

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

    @pytest.mark.parametrize(
        ('content_type', 'content_settings_type'),
        [(None, None), ('application/json', FakeContentSettings)],
    )
    def test_open_writes_binary_payload(
        self,
        monkeypatch: pytest.MonkeyPatch,
        content_type: str | None,
        content_settings_type: type[FakeContentSettings] | None,
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

        monkeypatch.setattr(backend, '_blob_client', lambda _location: FakeBlobClient())
        monkeypatch.setattr(
            azure_blob_mod,
            '_import_blob_types',
            lambda: (object, content_settings_type),
        )

        kwargs = {'content_type': content_type} if content_type else {}
        with backend.open(location, 'wb', **kwargs) as handle:
            handle.write(b'payload')

        assert uploads[0]['data'] == b'payload'
        assert uploads[0]['overwrite'] is True
        if content_type:
            assert isinstance(uploads[0]['content_settings'], FakeContentSettings)
            assert uploads[0]['content_settings'].content_type == content_type
        else:
            assert 'content_settings' not in uploads[0]

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

        monkeypatch.delenv('AZURE_STORAGE_CONNECTION_STRING', raising=False)
        monkeypatch.delenv('AZURE_STORAGE_ACCOUNT_URL', raising=False)
        monkeypatch.delenv('AZURE_STORAGE_CREDENTIAL', raising=False)
        monkeypatch.setattr(
            azure_blob_mod,
            '_import_blob_types',
            lambda: (object, None),
        )

        with pytest.raises(ValueError, match='AZURE_STORAGE_CONNECTION_STRING'):
            backend._service_client()

    @pytest.mark.parametrize(
        ('use_explicit_credential', 'env_credential'),
        [(False, 'secret'), (True, None)],
    )
    def test_service_client_uses_configured_credential(
        self,
        monkeypatch: pytest.MonkeyPatch,
        use_explicit_credential: bool,
        env_credential: str | None,
    ) -> None:
        """Test that Azure Blob forwards env and explicit credentials."""
        explicit_credential = object() if use_explicit_credential else None
        backend = AzureBlobStorageBackend(credential=explicit_credential)
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
        if env_credential is None:
            monkeypatch.delenv('AZURE_STORAGE_CREDENTIAL', raising=False)
        else:
            monkeypatch.setenv('AZURE_STORAGE_CREDENTIAL', env_credential)
        monkeypatch.setattr(
            azure_blob_mod,
            '_import_blob_types',
            lambda: (FakeBlobServiceClient, None),
        )

        backend._service_client(location)

        assert calls == [
            {
                'account_url': 'https://example.blob.core.windows.net',
                'credential': explicit_credential or env_credential,
            },
        ]

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
