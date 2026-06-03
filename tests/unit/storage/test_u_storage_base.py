"""
:mod:`tests.unit.storage.test_u_storage_base` module.

Unit tests for :mod:`etlplus.storage._base`.
"""

from __future__ import annotations

from typing import Any
from typing import cast

import pytest

from etlplus.storage import StorageBackendABC

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestStorageBackendABC:
    """Unit tests for :class:`etlplus.storage.StorageBackendABC`."""

    def test_storage_backend_abc_cannot_be_instantiated(self) -> None:
        """Test that the storage backend contract stays abstract."""
        backend_type = cast(Any, StorageBackendABC)
        with pytest.raises(TypeError, match='abstract class'):
            backend_type()

    def test_storage_backend_abc_declares_expected_abstract_methods(self) -> None:
        """Test that the backend contract exposes the expected methods."""
        assert StorageBackendABC.__abstractmethods__ == frozenset(
            {'delete', 'ensure_parent_dir', 'exists', 'open'},
        )
