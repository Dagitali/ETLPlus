"""
:mod:`tests.unit.storage.test_u_storage_remote` module.

Unit tests for :mod:`etlplus.storage._remote`.
"""

from __future__ import annotations

import pytest

from etlplus.storage import AbfsStorageBackend
from etlplus.storage import AzureBlobStorageBackend
from etlplus.storage import HdfsStorageBackend
from etlplus.storage import RemoteStorageBackend
from etlplus.storage import S3StorageBackend
from etlplus.storage import StorageLocation
from etlplus.storage import StorageScheme
from etlplus.storage import StubStorageBackend

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestRemoteStorageBackend:
    """Unit tests for shared remote-backend validation."""

    @pytest.mark.parametrize(
        'backend_type',
        [
            AbfsStorageBackend,
            AzureBlobStorageBackend,
            HdfsStorageBackend,
            S3StorageBackend,
        ],
    )
    def test_concrete_backends_use_remote_backend_base(
        self,
        backend_type: type[object],
    ) -> None:
        """Test concrete remote backends use the shared remote base class."""
        assert issubclass(backend_type, RemoteStorageBackend)
        assert not issubclass(backend_type, StubStorageBackend)

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

    def test_validate_rejects_wrong_scheme(self) -> None:
        """Test that remote backends reject locations with the wrong scheme."""
        backend = S3StorageBackend()
        location = StorageLocation.from_value('https://example.com/files/data.csv')

        with pytest.raises(TypeError, match='only supports'):
            backend.ensure_parent_dir(location)
