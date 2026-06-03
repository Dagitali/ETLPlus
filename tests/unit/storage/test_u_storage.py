"""
:mod:`tests.unit.storage.test_u_storage` module.

Unit tests for :mod:`etlplus.storage` package exports.
"""

from __future__ import annotations

import pytest

import etlplus.storage as storage_pkg
from etlplus.storage import AbfsStorageBackend
from etlplus.storage import AzureBlobStorageBackend
from etlplus.storage import FtpStorageBackend
from etlplus.storage import HdfsStorageBackend
from etlplus.storage import HttpStorageBackend
from etlplus.storage import LocalStorageBackend
from etlplus.storage import RemoteStorageBackend
from etlplus.storage import S3StorageBackend
from etlplus.storage import StorageBackendABC
from etlplus.storage import StorageLocation
from etlplus.storage import StorageScheme
from etlplus.storage import StubStorageBackend
from etlplus.storage import coerce_location
from etlplus.storage import get_backend

from ..pytest_export_contracts import assert_package_exports

# SECTION: HELPERS ========================================================== #


STORAGE_EXPORTS: tuple[tuple[str, object], ...] = (
    ('AbfsStorageBackend', AbfsStorageBackend),
    ('AzureBlobStorageBackend', AzureBlobStorageBackend),
    ('FtpStorageBackend', FtpStorageBackend),
    ('HdfsStorageBackend', HdfsStorageBackend),
    ('HttpStorageBackend', HttpStorageBackend),
    ('LocalStorageBackend', LocalStorageBackend),
    ('RemoteStorageBackend', RemoteStorageBackend),
    ('S3StorageBackend', S3StorageBackend),
    ('StubStorageBackend', StubStorageBackend),
    ('StorageBackendABC', StorageBackendABC),
    ('StorageLocation', StorageLocation),
    ('StorageScheme', StorageScheme),
    ('coerce_location', coerce_location),
    ('get_backend', get_backend),
)


# SECTION: TESTS ============================================================ #


class TestStoragePackageExports:
    """Unit tests for package-level exports."""

    @pytest.mark.parametrize(('name', 'expected'), STORAGE_EXPORTS)
    def test_expected_symbol_bindings(
        self,
        name: str,
        expected: object,
    ) -> None:
        """Test that package exports resolve to their canonical objects."""
        assert getattr(storage_pkg, name) == expected

    def test_expected_symbols(self) -> None:
        """Test that package facade preserves the documented export order."""
        assert_package_exports(
            package_module=storage_pkg,
            expected_exports=STORAGE_EXPORTS,
        )
