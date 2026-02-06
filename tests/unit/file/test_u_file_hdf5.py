"""
:mod:`tests.unit.file.test_u_file_hdf5` module.

Unit tests for :mod:`etlplus.file.hdf5`.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest

from etlplus.file import hdf5 as mod

# SECTION: HELPERS ========================================================== #


class _Frame:
    """Minimal frame stub for HDF5 helpers."""

    # pylint: disable=unused-argument

    def __init__(
        self,
        records: list[dict[str, object]],
    ) -> None:
        self._records = records

    def to_dict(
        self,
        *,
        orient: str,
    ) -> list[dict[str, object]]:  # noqa: ARG002
        """Simulate converting a frame to a list of records."""
        return list(self._records)


class _HDFStore:
    """Stub for pandas.HDFStore."""

    def __init__(self, keys: list[str], frames: dict[str, _Frame]) -> None:
        self._keys = keys
        self._frames = frames

    def __enter__(self) -> _HDFStore:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        return None

    def keys(self) -> list[str]:
        """Simulate returning the list of dataset keys in the store."""
        return [f'/{key}' for key in self._keys]

    def get(self, key: str) -> _Frame:
        """Simulate retrieving a dataset by key."""
        return self._frames[key]


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
        optional_module_stub({'pandas': _PandasStub(store)})

        assert mod.read(tmp_path / 'data.hdf5') == []

    def test_read_prefers_default_key(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """Test that reading prefers the default key when present."""
        frame = _Frame([{'id': 1}])
        store = _HDFStore(['data', 'other'], {'data': frame, 'other': frame})
        optional_module_stub({'pandas': _PandasStub(store)})

        assert mod.read(tmp_path / 'data.hdf5') == [{'id': 1}]

    def test_read_uses_single_key(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """Test that reading uses the single key when only one is present."""
        frame = _Frame([{'id': 1}])
        store = _HDFStore(['only'], {'only': frame})
        optional_module_stub({'pandas': _PandasStub(store)})

        assert mod.read(tmp_path / 'data.hdf5') == [{'id': 1}]

    def test_read_raises_on_multiple_keys(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """Test that reading raises when multiple keys are present."""
        frame = _Frame([{'id': 1}])
        store = _HDFStore(['a', 'b'], {'a': frame, 'b': frame})
        optional_module_stub({'pandas': _PandasStub(store)})

        with pytest.raises(ValueError, match='Multiple datasets'):
            mod.read(tmp_path / 'data.hdf5')


class TestHdf5Write:
    """Unit tests for :func:`etlplus.file.hdf5.write`."""

    def test_write_is_stubbed(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that writing raises :class:`NotImplementedError`."""
        with pytest.raises(NotImplementedError, match='HDF5 write'):
            mod.write(tmp_path / 'data.hdf5', [{'id': 1}])
