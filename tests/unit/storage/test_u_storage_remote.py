"""
:mod:`tests.unit.storage.test_u_storage_remote` module.

Unit tests for :mod:`etlplus.storage._remote`.
"""

from __future__ import annotations

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


# SECTION: CONSTANTS ======================================================== #

IMPLEMENTED_REMOTE_BACKEND_TYPES: tuple[RemoteBackendType, ...] = (
    AbfsStorageBackend,
    AzureBlobStorageBackend,
    HdfsStorageBackend,
    HttpStorageBackend,
    S3StorageBackend,
)

REMOTE_BACKEND_TYPES: tuple[RemoteBackendType, ...] = (
    AbfsStorageBackend,
    AzureBlobStorageBackend,
    FtpStorageBackend,
    HdfsStorageBackend,
    HttpStorageBackend,
    S3StorageBackend,
)

REMOTE_PROVIDER_CASES: tuple[
    tuple[
        RemoteBackendType,
        StorageScheme,
        str,
        str,
        str,
        str,
        str,
        str,
    ],
    ...,
] = (
    (
        AbfsStorageBackend,
        StorageScheme.ABFS,
        'filesystem@example.dfs.core.windows.net',
        'abfs://filesystem@example.dfs.core.windows.net',
        'filesystem/account authority',
        'filesystem path',
        'Azure Data Lake Storage Gen2',
        'abfs://filesystem@example.dfs.core.windows.net/data.csv',
    ),
    (
        AzureBlobStorageBackend,
        StorageScheme.AZURE_BLOB,
        'container',
        'azure-blob://container',
        'container name',
        'blob path',
        'Azure Blob',
        'azure-blob://container/data.csv',
    ),
    (
        FtpStorageBackend,
        StorageScheme.FTP,
        'example.com',
        'ftp://example.com',
        'host',
        'remote path',
        'FTP',
        'ftp://example.com/data.csv',
    ),
    (
        HttpStorageBackend,
        StorageScheme.HTTP,
        'example.com',
        'https://example.com',
        'host',
        'URL path',
        'HTTP',
        'https://example.com/data.csv',
    ),
    (
        S3StorageBackend,
        StorageScheme.S3,
        'bucket',
        's3://bucket',
        'bucket name',
        'object key',
        'S3',
        's3://bucket/data.csv',
    ),
)


# SECTION: TESTS ============================================================ #


class TestRemoteStorageBackend:
    """Unit tests for shared remote-backend validation."""

    @pytest.mark.parametrize(
        (
            'backend_type',
            'scheme',
            'authority',
            'missing_path_raw',
            'authority_label',
            'path_label',
            'service_name',
            'valid_raw',
        ),
        REMOTE_PROVIDER_CASES,
    )
    def test_backend_metadata_matches_provider_contract(
        self,
        backend_type: RemoteBackendType,
        scheme: StorageScheme,
        authority: str,
        missing_path_raw: str,
        authority_label: str,
        path_label: str,
        service_name: str,
        valid_raw: str,
    ) -> None:
        """Test that remote providers expose consistent validation metadata."""
        del authority, missing_path_raw, valid_raw
        assert backend_type.scheme is scheme
        assert backend_type.authority_label == authority_label
        assert backend_type.path_label == path_label
        assert backend_type.service_name == service_name

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

    @pytest.mark.parametrize(
        (
            'backend_type',
            'scheme',
            'authority',
            'missing_path_raw',
            'authority_label',
            'path_label',
            'service_name',
            'valid_raw',
        ),
        REMOTE_PROVIDER_CASES,
    )
    def test_validate_accepts_valid_remote_location(
        self,
        backend_type: RemoteBackendType,
        scheme: StorageScheme,
        authority: str,
        missing_path_raw: str,
        authority_label: str,
        path_label: str,
        service_name: str,
        valid_raw: str,
    ) -> None:
        """Test that remote backends accept well-formed provider locations."""
        del (
            scheme,
            authority,
            missing_path_raw,
            authority_label,
            path_label,
            service_name,
        )
        backend = backend_type()
        location = StorageLocation.from_value(valid_raw)

        backend.ensure_parent_dir(location)

    @pytest.mark.parametrize(
        (
            'backend_type',
            'scheme',
            'authority',
            'missing_path_raw',
            'authority_label',
            'path_label',
            'service_name',
            'valid_raw',
        ),
        REMOTE_PROVIDER_CASES,
    )
    def test_validate_requires_authority(
        self,
        backend_type: RemoteBackendType,
        scheme: StorageScheme,
        authority: str,
        missing_path_raw: str,
        authority_label: str,
        path_label: str,
        service_name: str,
        valid_raw: str,
    ) -> None:
        """Test that remote backends reject locations without authority."""
        del authority, missing_path_raw, path_label, service_name, valid_raw
        backend = backend_type()
        location = StorageLocation(
            raw=f'{scheme.value}:///data.csv',
            scheme=scheme,
            authority='',
            path='data.csv',
        )

        with pytest.raises(ValueError, match=authority_label):
            backend.ensure_parent_dir(location)

    @pytest.mark.parametrize(
        (
            'backend_type',
            'scheme',
            'authority',
            'missing_path_raw',
            'authority_label',
            'path_label',
            'service_name',
            'valid_raw',
        ),
        REMOTE_PROVIDER_CASES,
    )
    def test_validate_requires_path(
        self,
        backend_type: RemoteBackendType,
        scheme: StorageScheme,
        authority: str,
        missing_path_raw: str,
        authority_label: str,
        path_label: str,
        service_name: str,
        valid_raw: str,
    ) -> None:
        """Test that remote backends reject locations without resource paths."""
        del scheme, authority, authority_label, service_name, valid_raw
        backend = backend_type()
        location = StorageLocation.from_value(missing_path_raw)

        with pytest.raises(ValueError, match=path_label):
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
