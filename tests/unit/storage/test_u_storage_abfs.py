"""
:mod:`tests.unit.storage.test_u_storage_abfs` module.

Unit tests for :mod:`etlplus.storage._abfs`.
"""

from __future__ import annotations

import pytest

from etlplus.storage import AbfsStorageBackend
from etlplus.storage import StorageLocation
from etlplus.storage import _abfs as abfs_mod

from .pytest_storage_support import FakeContentSettings

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestAbfsStorageBackend:
    """Unit tests for :class:`etlplus.storage.AbfsStorageBackend`."""

    def test_account_url_from_authority_uses_account_host(self) -> None:
        """Test that ABFS account URLs are derived from the authority host."""
        backend = AbfsStorageBackend()
        assert (
            backend._account_url_from_authority(
                'filesystem@example.dfs.core.windows.net',
            )
            == 'https://example.dfs.core.windows.net'
        )

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

        monkeypatch.setattr(backend, '_file_client', lambda _location: FakeFileClient())
        backend.delete(location)
        assert deleted == [True]

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

    def test_exists_returns_false_when_file_client_reports_missing(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that ABFS existence checks preserve false client results."""
        backend = AbfsStorageBackend()
        location = StorageLocation.from_value(
            'abfs://filesystem@example.dfs.core.windows.net/missing.json',
        )

        class FakeFileClient:
            """Data Lake file client missing-file test double."""

            def exists(self) -> bool:
                """Return a missing-file result."""
                return False

        monkeypatch.setattr(backend, '_file_client', lambda _location: FakeFileClient())
        assert backend.exists(location) is False

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

        monkeypatch.setattr(backend, '_file_client', lambda _location: FakeFileClient())
        assert backend.exists(location) is True

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

        monkeypatch.setattr(backend, '_file_client', lambda _location: FakeFileClient())
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

    @pytest.mark.parametrize(
        ('content_type', 'content_settings_type'),
        [(None, None), ('application/octet-stream', FakeContentSettings)],
    )
    def test_open_writes_binary_payload(
        self,
        monkeypatch: pytest.MonkeyPatch,
        content_type: str | None,
        content_settings_type: type[FakeContentSettings] | None,
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

        monkeypatch.setattr(backend, '_file_client', lambda _location: FakeFileClient())
        monkeypatch.setattr(
            abfs_mod,
            '_import_datalake_types',
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

        monkeypatch.delenv('AZURE_STORAGE_CONNECTION_STRING', raising=False)
        monkeypatch.delenv('AZURE_STORAGE_ACCOUNT_URL', raising=False)
        monkeypatch.delenv('AZURE_STORAGE_CREDENTIAL', raising=False)
        monkeypatch.setattr(
            abfs_mod,
            '_import_datalake_types',
            lambda: (object, None),
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
        """Test that ABFS forwards env and explicit credentials."""
        explicit_credential = object() if use_explicit_credential else None
        backend = AbfsStorageBackend(credential=explicit_credential)
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
        if env_credential is None:
            monkeypatch.delenv('AZURE_STORAGE_CREDENTIAL', raising=False)
        else:
            monkeypatch.setenv('AZURE_STORAGE_CREDENTIAL', env_credential)
        monkeypatch.setattr(
            abfs_mod,
            '_import_datalake_types',
            lambda: (FakeServiceClient, None),
        )

        backend._service_client(location)

        assert calls == [
            {
                'account_url': 'https://example.dfs.core.windows.net',
                'credential': explicit_credential or env_credential,
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
