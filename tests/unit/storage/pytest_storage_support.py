"""
:mod:`tests.unit.storage.pytest_storage_support` module.

Shared test doubles for storage unit tests.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import pytest

from etlplus.storage import AbfsStorageBackend
from etlplus.storage import AzureBlobStorageBackend
from etlplus.storage import FtpStorageBackend
from etlplus.storage import HdfsStorageBackend
from etlplus.storage import HttpStorageBackend
from etlplus.storage import RemoteStorageBackend
from etlplus.storage import S3StorageBackend
from etlplus.storage import StorageLocation
from etlplus.storage import StorageScheme

# SECTION: TYPE ALIASES ===================================================== #

RemoteBackendType = type[RemoteStorageBackend]
RemoteValidationKind = Literal['missing_authority', 'missing_path']


# SECTION: DATA CLASSES ===================================================== #


@dataclass(slots=True)
class FakeContentSettings:
    """Minimal content-settings test double."""

    content_type: str


@dataclass(frozen=True, slots=True)
class RemoteProviderCase:
    """Provider metadata used by shared remote-backend contract tests."""

    backend_type: RemoteBackendType
    scheme: StorageScheme
    authority_label: str
    path_label: str
    service_name: str
    package_name: str | None
    missing_path_raw: str
    valid_raw: str
    uses_stub_base: bool = False

    def invalid_location(
        self,
        validation_kind: RemoteValidationKind,
    ) -> tuple[StorageLocation, str]:
        """Return one invalid location and the expected validation-message text."""
        if validation_kind == 'missing_authority':
            return (
                StorageLocation(
                    raw=f'{self.scheme.value}:///data.csv',
                    scheme=self.scheme,
                    authority='',
                    path='data.csv',
                ),
                self.authority_label,
            )
        return StorageLocation.from_value(self.missing_path_raw), self.path_label


# SECTION: CONSTANTS ======================================================== #


REMOTE_BACKEND_TYPES: tuple[RemoteBackendType, ...] = (
    AbfsStorageBackend,
    AzureBlobStorageBackend,
    FtpStorageBackend,
    HdfsStorageBackend,
    HttpStorageBackend,
    S3StorageBackend,
)

REMOTE_PROVIDER_VALIDATION_KINDS: tuple[RemoteValidationKind, ...] = (
    'missing_authority',
    'missing_path',
)


REMOTE_PROVIDER_CASES: tuple[RemoteProviderCase, ...] = (
    RemoteProviderCase(
        backend_type=AbfsStorageBackend,
        scheme=StorageScheme.ABFS,
        authority_label='filesystem/account authority',
        path_label='filesystem path',
        service_name='Azure Data Lake Storage Gen2',
        package_name='azure-storage-file-datalake',
        missing_path_raw='abfs://filesystem@example.dfs.core.windows.net',
        valid_raw='abfs://filesystem@example.dfs.core.windows.net/data.csv',
    ),
    RemoteProviderCase(
        backend_type=AzureBlobStorageBackend,
        scheme=StorageScheme.AZURE_BLOB,
        authority_label='container name',
        path_label='blob path',
        service_name='Azure Blob',
        package_name='azure-storage-blob',
        missing_path_raw='azure-blob://container',
        valid_raw='azure-blob://container/data.csv',
    ),
    RemoteProviderCase(
        backend_type=FtpStorageBackend,
        scheme=StorageScheme.FTP,
        authority_label='host',
        path_label='remote path',
        service_name='FTP',
        package_name='ftplib',
        missing_path_raw='ftp://example.com',
        valid_raw='ftp://example.com/data.csv',
        uses_stub_base=True,
    ),
    RemoteProviderCase(
        backend_type=HttpStorageBackend,
        scheme=StorageScheme.HTTP,
        authority_label='host',
        path_label='URL path',
        service_name='HTTP',
        package_name=None,
        missing_path_raw='https://example.com',
        valid_raw='https://example.com/data.csv',
    ),
    RemoteProviderCase(
        backend_type=S3StorageBackend,
        scheme=StorageScheme.S3,
        authority_label='bucket name',
        path_label='object key',
        service_name='S3',
        package_name='boto3',
        missing_path_raw='s3://bucket',
        valid_raw='s3://bucket/data.csv',
    ),
)


# SECTION: FUNCTIONS ======================================================== #


def clear_azure_storage_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Clear Azure storage configuration environment variables."""
    for name in (
        'AZURE_STORAGE_CONNECTION_STRING',
        'AZURE_STORAGE_ACCOUNT_URL',
        'AZURE_STORAGE_CREDENTIAL',
    ):
        monkeypatch.delenv(name, raising=False)


def assert_upload_payload(
    uploads: list[dict[str, object]],
    *,
    content_type: str | None,
) -> None:
    """Assert one buffered upload payload and optional content settings."""
    (upload,) = uploads
    assert upload['data'] == b'payload'
    assert upload['overwrite'] is True
    if content_type:
        content_settings = upload['content_settings']
        assert isinstance(content_settings, FakeContentSettings)
        assert content_settings.content_type == content_type
    else:
        assert 'content_settings' not in upload
