"""
:mod:`tests.unit.file.test_u_file_hdf5` module.

Unit tests for :mod:`etlplus.file.hdf5`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from etlplus.file import hdf5 as mod
from etlplus.file.base import ReadOptions
from tests.unit.file.conftest import ContextManagerSelfMixin
from tests.unit.file.conftest import DictRecordsFrameStub
from tests.unit.file.conftest import OptionalModuleInstaller
from tests.unit.file.conftest import PathMixin
from tests.unit.file.conftest import ReadOnlyScientificDatasetModuleContract
from tests.unit.file.conftest import patch_dependency_resolver_value

# SECTION: HELPERS ========================================================== #


class _HDFStore(ContextManagerSelfMixin):
    """Stub for pandas.HDFStore."""

    def __init__(
        self,
        keys: list[str],
        frames: dict[str, DictRecordsFrameStub],
    ) -> None:
        self._keys = keys
        self._frames = frames

    def get(self, key: str) -> DictRecordsFrameStub:
        """Simulate retrieving a dataset by key."""
        return self._frames[key]

    def keys(self) -> list[str]:
        """Simulate returning the list of dataset keys in the store."""
        return [f'/{key}' for key in self._keys]


class _PandasStub:
    """Stub for pandas module."""

    # pylint: disable=invalid-name, unused-argument

    def __init__(
        self,
        store: _HDFStore | None = None,
    ) -> None:
        self._store = store

    def HDFStore(
        self,
        path: Path,
    ) -> _HDFStore:  # noqa: ARG002
        """Simulate opening an HDF5 store."""
        if self._store is None:
            raise ImportError('missing tables')
        return self._store


# SECTION: TESTS ============================================================ #


class TestHdf5Datasets(PathMixin):
    """Unit tests for dataset-specific HDF5 handler methods."""

    format_name = 'hdf5'

    def test_list_datasets_raises_when_tables_missing(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test list_datasets raising a tables dependency ImportError."""
        patch_dependency_resolver_value(
            monkeypatch,
            mod,
            resolver_name='get_pandas',
            value=_PandasStub(store=None),
        )
        with pytest.raises(ImportError, match='tables'):
            mod.Hdf5File().list_datasets(self.format_path(tmp_path))

    def test_list_datasets_strips_leading_slashes(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test list_datasets returning normalized dataset keys."""
        frame = DictRecordsFrameStub([{'id': 1}])
        store = _HDFStore(['data', 'other'], {'data': frame, 'other': frame})
        TestHdf5Read._install_store(optional_module_stub, store)

        result = mod.Hdf5File().list_datasets(self.format_path(tmp_path))

        assert result == ['data', 'other']

    def test_read_dataset_uses_explicit_dataset_argument(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test read_dataset honoring explicit dataset selection."""
        default_frame = DictRecordsFrameStub([{'id': 1}])
        other_frame = DictRecordsFrameStub([{'id': 2}])
        store = _HDFStore(
            ['data', 'other'],
            {'data': default_frame, 'other': other_frame},
        )
        TestHdf5Read._install_store(optional_module_stub, store)

        result = mod.Hdf5File().read_dataset(
            self.format_path(tmp_path),
            dataset='other',
        )

        assert result == [{'id': 2}]

    def test_read_dataset_uses_options_dataset(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test read_dataset honoring dataset selector from read options."""
        default_frame = DictRecordsFrameStub([{'id': 1}])
        other_frame = DictRecordsFrameStub([{'id': 2}])
        store = _HDFStore(
            ['data', 'other'],
            {'data': default_frame, 'other': other_frame},
        )
        TestHdf5Read._install_store(optional_module_stub, store)

        result = mod.Hdf5File().read_dataset(
            self.format_path(tmp_path),
            options=ReadOptions(dataset='other'),
        )

        assert result == [{'id': 2}]

    def test_write_dataset_uses_read_only_write_contract(
        self,
        tmp_path: Path,
    ) -> None:
        """Test write_dataset still raising the read-only RuntimeError."""
        with pytest.raises(RuntimeError, match='read-only'):
            mod.Hdf5File().write_dataset(
                self.format_path(tmp_path),
                [{'id': 1}],
                dataset='other',
            )


class TestHdf5Read(PathMixin):
    """Unit tests for :func:`etlplus.file.hdf5.read`."""

    format_name = 'hdf5'

    def test_read_prefers_default_key(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test that reading prefers the default key when present."""
        frame = DictRecordsFrameStub([{'id': 1}])
        store = _HDFStore(['data', 'other'], {'data': frame, 'other': frame})
        self._install_store(optional_module_stub, store)
        path = self.format_path(tmp_path)

        assert mod.Hdf5File().read(path) == [{'id': 1}]

    def test_read_raises_on_multiple_keys(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test that reading raises when multiple keys are present."""
        frame = DictRecordsFrameStub([{'id': 1}])
        store = _HDFStore(['a', 'b'], {'a': frame, 'b': frame})
        self._install_store(optional_module_stub, store)
        path = self.format_path(tmp_path)

        with pytest.raises(ValueError, match='Multiple datasets'):
            mod.Hdf5File().read(path)

    def test_read_raises_when_tables_missing(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that reading raises when the HDF5 store has no tables."""
        pandas = _PandasStub(store=None)
        patch_dependency_resolver_value(
            monkeypatch,
            mod,
            resolver_name='get_pandas',
            value=pandas,
        )
        path = self.format_path(tmp_path)

        with pytest.raises(ImportError, match='tables'):
            mod.Hdf5File().read(path)

    def test_read_returns_empty_when_no_keys(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test that reading returns an empty list when no keys are present."""
        store = _HDFStore([], {})
        self._install_store(optional_module_stub, store)
        path = self.format_path(tmp_path)

        assert mod.Hdf5File().read(path) == []

    def test_read_uses_single_key(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test that reading uses the single key when only one is present."""
        frame = DictRecordsFrameStub([{'id': 1}])
        store = _HDFStore(['only'], {'only': frame})
        self._install_store(optional_module_stub, store)
        path = self.format_path(tmp_path)

        assert mod.Hdf5File().read(path) == [{'id': 1}]

    @staticmethod
    def _install_store(
        optional_module_stub: OptionalModuleInstaller,
        store: _HDFStore,
    ) -> None:
        """Install one HDFStore-backed pandas stub."""
        optional_module_stub({'pandas': _PandasStub(store)})


class TestHdf5ReadOnly(ReadOnlyScientificDatasetModuleContract):
    """Read-only scientific contract tests for :mod:`etlplus.file.hdf5`."""

    module = mod
    handler_cls = mod.Hdf5File
    format_name = 'hdf5'
    unknown_dataset_error_pattern = 'not found'

    def prepare_unknown_dataset_env(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,  # noqa: ARG002
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Install a pandas store stub for unknown-dataset checks."""
        _ = tmp_path
        frame = DictRecordsFrameStub([{'id': 1}])
        store = _HDFStore(['data'], {'data': frame})
        optional_module_stub({'pandas': _PandasStub(store)})
