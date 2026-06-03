"""
:mod:`tests.unit.storage.test_u_storage_stub` module.

Unit tests for :mod:`etlplus.storage._stub`.
"""

from __future__ import annotations

import pytest

from etlplus.storage import StorageLocation
from etlplus.storage import StorageScheme
from etlplus.storage import StubStorageBackend
from etlplus.storage import _stub as stub_mod

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


class DemoStubStorageBackend(StubStorageBackend):
    """Concrete storage stub used to exercise shared placeholder behavior."""

    authority_label = 'bucket'
    package_name = 'demo-sdk'
    path_label = 'object key'
    scheme = StorageScheme.S3
    service_name = 'Demo'


# SECTION: TESTS ============================================================ #


class TestStubStorageBackend:
    """Unit tests for :class:`etlplus.storage.StubStorageBackend`."""

    def test_raise_not_implemented_formats_service_and_package(self) -> None:
        """Test that placeholder errors name the missing implementation."""
        with pytest.raises(NotImplementedError, match='Demo.*demo-sdk'):
            stub_mod._raise_not_implemented('Demo', package_name='demo-sdk')

    @pytest.mark.parametrize(
        ('method_name', 'args', 'kwargs'),
        [
            ('exists', (), {}),
            ('delete', (), {}),
            ('open', ('rb',), {'newline': None}),
        ],
    )
    def test_runtime_operations_validate_then_raise_placeholder_error(
        self,
        method_name: str,
        args: tuple[object, ...],
        kwargs: dict[str, object],
    ) -> None:
        """Test that shared stub operations fail closed after validation."""
        backend = DemoStubStorageBackend()
        location = StorageLocation.from_value('s3://bucket/data.json')

        with pytest.raises(NotImplementedError, match='demo-sdk'):
            getattr(backend, method_name)(location, *args, **kwargs)
