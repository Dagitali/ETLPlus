"""
:mod:`tests.unit.storage.test_u_storage_registry` module.

Unit tests for :mod:`etlplus.storage._registry`.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from etlplus.storage import AbfsStorageBackend
from etlplus.storage import AzureBlobStorageBackend
from etlplus.storage import FtpStorageBackend
from etlplus.storage import HdfsStorageBackend
from etlplus.storage import HttpStorageBackend
from etlplus.storage import LocalStorageBackend
from etlplus.storage import S3StorageBackend
from etlplus.storage import StorageLocation
from etlplus.storage import _registry as registry_mod
from etlplus.storage import coerce_location
from etlplus.storage import get_backend

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestStorageRegistry:
    """Unit tests for storage backend resolution helpers."""

    def test_coerce_location_preserves_existing_instance(self) -> None:
        """
        Test that :func:`coerce_location` returns existing instances as-is.
        """
        location = StorageLocation.from_value('data/input.csv')
        assert coerce_location(location) is location

    @pytest.mark.parametrize(
        ('raw', 'backend_type'),
        [
            (
                'abfs://filesystem@example.dfs.core.windows.net/path.json',
                AbfsStorageBackend,
            ),
            ('azure-blob://container/path.json', AzureBlobStorageBackend),
            ('ftp://example.com/path.json', FtpStorageBackend),
            ('https://example.com/files/data.csv', HttpStorageBackend),
            ('hdfs://namenode.example.com:8020/path.json', HdfsStorageBackend),
            ('data/input.csv', LocalStorageBackend),
            ('s3://bucket/path.json', S3StorageBackend),
        ],
    )
    def test_get_backend_for_location(
        self,
        raw: str,
        backend_type: type[object],
    ) -> None:
        """Test that storage locations resolve to the expected backend type."""
        assert isinstance(get_backend(raw), backend_type)

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
