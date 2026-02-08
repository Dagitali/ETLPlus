"""
:mod:`tests.unit.file.test_u_file_nc` module.

Unit tests for :mod:`etlplus.file.nc`.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest

from etlplus.file import nc as mod
from tests.unit.file.conftest import SingleDatasetWritableContract

# SECTION: HELPERS ========================================================== #


class _Frame:
    """Minimal frame stub to support NC helpers."""

    # pylint: disable=unused-argument

    def __init__(self, records: list[dict[str, object]]) -> None:
        self._records = list(records)
        self.columns = list(records[0].keys()) if records else []

    def reset_index(self) -> _Frame:
        """
        Simulate resetting the index by ignoring any 'index' column and
        returning a new frame.
        """
        return self

    def __getitem__(self, key: str) -> list[object]:
        return [row.get(key) for row in self._records]

    def drop(
        self,
        columns: list[str],
    ) -> _Frame:
        """
        Simulate dropping columns by returning a new frame with those keys
        removed.
        """
        remaining = [
            {k: v for k, v in row.items() if k not in columns}
            for row in self._records
        ]
        return _Frame(remaining)

    def to_dict(
        self,
        *,
        orient: str,
    ) -> list[dict[str, object]]:  # noqa: ARG002
        """
        Simulate converting to a dictionary with a specific orientation.
        """
        return list(self._records)


class _Dataset:
    """Stub dataset supporting context management."""

    # pylint: disable=unused-argument

    def __init__(self, frame: _Frame, *, fail: bool = False) -> None:
        self._frame = frame
        self._fail = fail
        self.to_netcdf_calls: list[Path] = []

    def __enter__(self) -> _Dataset:
        return self

    def __exit__(
        self,
        exc_type,
        exc,
        tb,
    ) -> None:  # noqa: ANN001
        """Support context management."""
        return None

    def to_dataframe(self) -> _Frame:
        """
        Simulate converting to a DataFrame by returning the stored frame.
        """
        return self._frame

    def to_netcdf(
        self,
        path: Path,
    ) -> None:
        """
        Simulate writing to a NetCDF file by recording the path.
        """
        if self._fail:
            raise ImportError('engine missing')
        self.to_netcdf_calls.append(Path(path))


class _XarrayStub:
    """Stub for xarray module."""

    # pylint: disable=unused-argument

    def __init__(self, dataset: _Dataset) -> None:
        self._dataset = dataset
        self._from_frame: list[object] = []

    def open_dataset(
        self,
        path: Path,
    ) -> _Dataset:  # noqa: ARG002
        """Simulate opening a dataset by returning the stored dataset."""
        return self._dataset

    class Dataset:  # noqa: D106
        """Simulate :class:`xarray.Dataset` with a from_dataframe method."""

        @staticmethod
        def from_dataframe(frame: object) -> _Dataset:
            """Simulate creating a dataset from a :class:`pandas.DataFrame`."""
            raise AssertionError('Dataset.from_dataframe not patched')


class _PandasStub:
    """Stub for pandas module."""

    class DataFrame:  # noqa: D106
        """Simulate :class:`pandas.DataFrame` with from_records method."""

        @staticmethod
        def from_records(
            records: list[dict[str, object]],
        ) -> _Frame:
            """Simulate creating a DataFrame from records."""
            return _Frame(records)


# SECTION: TESTS ============================================================ #


class TestNc(SingleDatasetWritableContract):
    """Unit tests for :mod:`etlplus.file.nc`."""

    module = mod
    handler_cls = mod.NcFile
    format_name = 'nc'

    def test_read_keeps_non_sequential_index(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """Test that reading keeps a non-sequential index."""
        frame = _Frame([{'index': 2, 'value': 10}, {'index': 4, 'value': 11}])
        dataset = _Dataset(frame)
        xarray = _XarrayStub(dataset)
        optional_module_stub({'xarray': xarray, 'pandas': _PandasStub()})

        result = mod.read(tmp_path / 'data.nc')

        assert result == [
            {'index': 2, 'value': 10},
            {'index': 4, 'value': 11},
        ]

    def test_read_drops_sequential_index(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """Test that reading drops a sequential index."""
        frame = _Frame([{'index': 0, 'value': 1}, {'index': 1, 'value': 2}])
        dataset = _Dataset(frame)
        xarray = _XarrayStub(dataset)
        optional_module_stub({'xarray': xarray, 'pandas': _PandasStub()})
        path = tmp_path / 'data.nc'

        result = mod.read(path)

        assert result == [{'value': 1}, {'value': 2}]

    def test_read_raises_engine_error(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that reading raises an engine error."""

        def _open_dataset(_: Path) -> _Dataset:
            raise ImportError('engine missing')

        xarray = _XarrayStub(_Dataset(_Frame([])))
        xarray.open_dataset = _open_dataset  # type: ignore[assignment]
        monkeypatch.setattr(mod, 'get_dependency', lambda *_, **__: xarray)

        with pytest.raises(ImportError, match='NC support requires optional'):
            mod.read(tmp_path / 'data.nc')

    def test_write_raises_engine_error(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that writing raises an engine error."""
        frame = _Frame([{'value': 1}])
        dataset = _Dataset(frame, fail=True)
        xarray = _XarrayStub(dataset)

        def _from_dataframe(_: object) -> _Dataset:
            return dataset

        monkeypatch.setattr(
            xarray.Dataset,
            'from_dataframe',
            staticmethod(_from_dataframe),
        )
        optional_module_stub({'xarray': xarray, 'pandas': _PandasStub()})

        with pytest.raises(ImportError, match='NC support requires optional'):
            mod.write(tmp_path / 'data.nc', [{'value': 1}])

    def test_write_happy_path(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test the happy path of writing data to a NetCDF file."""
        frame = _Frame([{'value': 1}])
        dataset = _Dataset(frame)
        xarray = _XarrayStub(dataset)

        def _from_dataframe(_: object) -> _Dataset:
            return dataset

        monkeypatch.setattr(
            xarray.Dataset,
            'from_dataframe',
            staticmethod(_from_dataframe),
        )
        optional_module_stub({'xarray': xarray, 'pandas': _PandasStub()})
        path = tmp_path / 'data.nc'

        written = mod.write(path, [{'value': 1}])

        assert written == 1
        assert dataset.to_netcdf_calls == [path]
