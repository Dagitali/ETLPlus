"""
:mod:`tests.unit.storage.test_u_storage_remote` module.

Unit tests for :mod:`etlplus.storage._remote`.
"""

from __future__ import annotations

import pytest

from etlplus.storage import RemoteStorageBackend
from etlplus.storage import StorageLocation
from etlplus.storage import StubStorageBackend

from .pytest_storage_support import REMOTE_BACKEND_TYPES
from .pytest_storage_support import REMOTE_PROVIDER_CASES
from .pytest_storage_support import REMOTE_PROVIDER_VALIDATION_KINDS
from .pytest_storage_support import RemoteBackendType
from .pytest_storage_support import RemoteProviderCase
from .pytest_storage_support import RemoteValidationKind

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TYPE ALIASES ===================================================== #

RemoteOperationCase = tuple[str, tuple[object, ...], dict[str, object]]


# SECTION: CONSTANTS ======================================================== #

REMOTE_OPERATION_CASES: tuple[RemoteOperationCase, ...] = (
    ('delete', (), {}),
    ('exists', (), {}),
    ('open', ('rb',), {}),
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

    @pytest.mark.parametrize('case', REMOTE_PROVIDER_CASES)
    @pytest.mark.parametrize('validation_kind', REMOTE_PROVIDER_VALIDATION_KINDS)
    @pytest.mark.parametrize(('method_name', 'args', 'kwargs'), REMOTE_OPERATION_CASES)
    def test_operations_reject_invalid_locations_before_runtime_behavior(
        self,
        case: RemoteProviderCase,
        validation_kind: RemoteValidationKind,
        method_name: str,
        args: tuple[object, ...],
        kwargs: dict[str, object],
    ) -> None:
        """Test public remote operations reject invalid locations consistently."""
        backend = case.backend_type()
        location, match = case.invalid_location(validation_kind)

        with pytest.raises(ValueError, match=match):
            getattr(backend, method_name)(location, *args, **kwargs)

    @pytest.mark.parametrize('backend_type', REMOTE_BACKEND_TYPES)
    @pytest.mark.parametrize(('method_name', 'args', 'kwargs'), REMOTE_OPERATION_CASES)
    def test_operations_reject_wrong_scheme_before_runtime_behavior(
        self,
        backend_type: RemoteBackendType,
        method_name: str,
        args: tuple[object, ...],
        kwargs: dict[str, object],
    ) -> None:
        """Test public remote operations reject wrong schemes consistently."""
        backend = backend_type()
        location = StorageLocation.from_value('data.csv')

        with pytest.raises(TypeError, match='only supports'):
            getattr(backend, method_name)(location, *args, **kwargs)

    @pytest.mark.parametrize('case', REMOTE_PROVIDER_CASES)
    def test_provider_stub_status_matches_contract(
        self,
        case: RemoteProviderCase,
    ) -> None:
        """Test provider placeholder status matches shared backend contracts."""
        assert issubclass(case.backend_type, StubStorageBackend) is case.uses_stub_base

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
    @pytest.mark.parametrize('validation_kind', REMOTE_PROVIDER_VALIDATION_KINDS)
    def test_validate_rejects_invalid_locations(
        self,
        case: RemoteProviderCase,
        validation_kind: RemoteValidationKind,
    ) -> None:
        """Test that remote backends reject invalid locations."""
        backend = case.backend_type()
        location, match = case.invalid_location(validation_kind)

        with pytest.raises(ValueError, match=match):
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
