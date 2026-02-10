"""
:mod:`tests.unit.file.test_u_file_hdf5` module.

Unit tests for :mod:`etlplus.file.hdf5`.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest

from etlplus.file import hdf5 as mod
from tests.unit.file.conftest import ContextManagerSelfMixin
from tests.unit.file.conftest import DictRecordsFrameStub
from tests.unit.file.conftest import ReadOnlyScientificDatasetModuleContract

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
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """Install a pandas store stub for unknown-dataset checks."""
        _ = tmp_path
        frame = DictRecordsFrameStub([{'id': 1}])
        store = _HDFStore(['data'], {'data': frame})
        optional_module_stub({'pandas': _PandasStub(store)})


class TestHdf5Read:
    """Unit tests for :func:`etlplus.file.hdf5.read`."""

    def test_read_raises_when_tables_missing(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that reading raises when the HDF5 store has no tables."""
        pandas = _PandasStub(store=None)
        monkeypatch.setattr(mod, 'get_pandas', lambda *_: pandas)

        with pytest.raises(ImportError, match='tables'):
            mod.read(tmp_path / 'data.hdf5')

    def test_read_returns_empty_when_no_keys(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """Test that reading returns an empty list when no keys are present."""
        store = _HDFStore([], {})
        self._install_store(optional_module_stub, store)

        assert mod.read(tmp_path / 'data.hdf5') == []

    def test_read_prefers_default_key(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """Test that reading prefers the default key when present."""
        frame = DictRecordsFrameStub([{'id': 1}])
        store = _HDFStore(['data', 'other'], {'data': frame, 'other': frame})
        self._install_store(optional_module_stub, store)

        assert mod.read(tmp_path / 'data.hdf5') == [{'id': 1}]

    def test_read_raises_on_multiple_keys(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """Test that reading raises when multiple keys are present."""
        frame = DictRecordsFrameStub([{'id': 1}])
        store = _HDFStore(['a', 'b'], {'a': frame, 'b': frame})
        self._install_store(optional_module_stub, store)

        with pytest.raises(ValueError, match='Multiple datasets'):
            mod.read(tmp_path / 'data.hdf5')

    def test_read_uses_single_key(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """Test that reading uses the single key when only one is present."""
        frame = DictRecordsFrameStub([{'id': 1}])
        store = _HDFStore(['only'], {'only': frame})
        self._install_store(optional_module_stub, store)

        assert mod.read(tmp_path / 'data.hdf5') == [{'id': 1}]

    @staticmethod
    def _install_store(
        optional_module_stub: Callable[[dict[str, object]], None],
        store: _HDFStore,
    ) -> None:
        """Install one HDFStore-backed pandas stub."""
        optional_module_stub({'pandas': _PandasStub(store)})
