"""
:mod:`tests.unit.storage.test_u_storage_remote` module.

Unit tests for :mod:`etlplus.storage._remote`.
"""

from __future__ import annotations

from dataclasses import dataclass

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
from etlplus.storage import StubStorageBackend

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TYPE ALIASES ===================================================== #

RemoteBackendType = type[RemoteStorageBackend]


# SECTION: DATA CLASSES ===================================================== #


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


# SECTION: CONSTANTS ======================================================== #

IMPLEMENTED_REMOTE_BACKEND_TYPES: tuple[RemoteBackendType, ...] = (
    AbfsStorageBackend,
    AzureBlobStorageBackend,
    HdfsStorageBackend,
    HttpStorageBackend,
    S3StorageBackend,
)

PLACEHOLDER_REMOTE_BACKEND_TYPES: tuple[RemoteBackendType, ...] = (FtpStorageBackend,)

REMOTE_BACKEND_TYPES: tuple[RemoteBackendType, ...] = (
    AbfsStorageBackend,
    AzureBlobStorageBackend,
    FtpStorageBackend,
    HdfsStorageBackend,
    HttpStorageBackend,
    S3StorageBackend,
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


# SECTION: TESTS ============================================================ #


class TestRemoteStorageBackend:
    """Unit tests for shared remote-backend validation."""

    @pytest.mark.parametrize('case', REMOTE_PROVIDER_CASES)
    def test_backend_metadata_matches_provider_contract(
        self,
        case: RemoteProviderCase,
    ) -> None:
        """Test that remote providers expose consistent validation metadata."""
        assert case.backend_type.scheme is case.scheme
        assert case.backend_type.authority_label == case.authority_label
        assert case.backend_type.path_label == case.path_label
        assert case.backend_type.service_name == case.service_name
        assert getattr(case.backend_type, 'package_name', None) == case.package_name

    @pytest.mark.parametrize('backend_type', REMOTE_BACKEND_TYPES)
    def test_concrete_backends_use_remote_backend_base(
        self,
        backend_type: RemoteBackendType,
    ) -> None:
        """Test concrete remote backends use the shared remote base class."""
        assert issubclass(backend_type, RemoteStorageBackend)

    @pytest.mark.parametrize('backend_type', IMPLEMENTED_REMOTE_BACKEND_TYPES)
    def test_implemented_backends_do_not_use_stub_base(
        self,
        backend_type: RemoteBackendType,
    ) -> None:
        """Test implemented remote backends do not use placeholder behavior."""
        assert not issubclass(backend_type, StubStorageBackend)

    @pytest.mark.parametrize('backend_type', PLACEHOLDER_REMOTE_BACKEND_TYPES)
    def test_placeholder_backends_use_stub_base(
        self,
        backend_type: RemoteBackendType,
    ) -> None:
        """Test placeholder remote backends use shared stub behavior."""
        assert issubclass(backend_type, StubStorageBackend)

    @pytest.mark.parametrize('case', REMOTE_PROVIDER_CASES)
    def test_validate_accepts_valid_remote_location(
        self,
        case: RemoteProviderCase,
    ) -> None:
        """Test that remote backends accept well-formed provider locations."""
        backend = case.backend_type()
        location = StorageLocation.from_value(case.valid_raw)

        backend.ensure_parent_dir(location)

    @pytest.mark.parametrize('case', REMOTE_PROVIDER_CASES)
    def test_validate_requires_authority(
        self,
        case: RemoteProviderCase,
    ) -> None:
        """Test that remote backends reject locations without authority."""
        backend = case.backend_type()
        location = StorageLocation(
            raw=f'{case.scheme.value}:///data.csv',
            scheme=case.scheme,
            authority='',
            path='data.csv',
        )

        with pytest.raises(ValueError, match=case.authority_label):
            backend.ensure_parent_dir(location)

    @pytest.mark.parametrize('case', REMOTE_PROVIDER_CASES)
    def test_validate_requires_path(
        self,
        case: RemoteProviderCase,
    ) -> None:
        """Test that remote backends reject locations without resource paths."""
        backend = case.backend_type()
        location = StorageLocation.from_value(case.missing_path_raw)

        with pytest.raises(ValueError, match=case.path_label):
            backend.ensure_parent_dir(location)

    @pytest.mark.parametrize('backend_type', REMOTE_BACKEND_TYPES)
    def test_validate_rejects_wrong_scheme(
        self,
        backend_type: RemoteBackendType,
    ) -> None:
        """Test that remote backends reject locations with the wrong scheme."""
        backend = backend_type()
        location = StorageLocation.from_value('data.csv')

        with pytest.raises(TypeError, match='only supports'):
            backend.ensure_parent_dir(location)
