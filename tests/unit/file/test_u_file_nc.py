"""
:mod:`tests.unit.file.test_u_file_nc` module.

Unit tests for :mod:`etlplus.file.nc`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from etlplus.file import nc as mod
from tests.unit.file.pytest_file_contract_contracts import (
    SingleDatasetWritableContract,
)
from tests.unit.file.pytest_file_contract_mixins import OptionalModuleInstaller
from tests.unit.file.pytest_file_contract_utils import (
    patch_dependency_resolver_value,
)
from tests.unit.file.pytest_file_support import ContextManagerSelfMixin
from tests.unit.file.pytest_file_support import DictRecordsFrameStub
from tests.unit.file.pytest_file_support import RDataPandasStub

# SECTION: HELPERS ========================================================== #


class _Dataset(ContextManagerSelfMixin):
    """Stub dataset supporting context management."""

    # pylint: disable=unused-argument

    def __init__(
        self,
        frame: DictRecordsFrameStub,
        *,
        fail: bool = False,
    ) -> None:
        self._frame = frame
        self._fail = fail
        self.to_netcdf_calls: list[Path] = []

    def to_dataframe(self) -> DictRecordsFrameStub:
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


# SECTION: TESTS ============================================================ #


class TestNc(SingleDatasetWritableContract):
    """Unit tests for :mod:`etlplus.file.nc`."""

    module = mod
    handler_cls = mod.NcFile
    format_name = 'nc'

    def test_read_drops_sequential_index(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test that reading drops a sequential index."""
        frame = DictRecordsFrameStub(
            [{'index': 0, 'value': 1}, {'index': 1, 'value': 2}],
        )
        dataset = _Dataset(frame)
        xarray = _XarrayStub(dataset)
        optional_module_stub({'xarray': xarray})
        path = self.format_path(tmp_path)

        result = mod.NcFile().read(path)

        assert result == [{'value': 1}, {'value': 2}]

    def test_read_keeps_non_sequential_index(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test that reading keeps a non-sequential index."""
        frame = DictRecordsFrameStub(
            [{'index': 2, 'value': 10}, {'index': 4, 'value': 11}],
        )
        dataset = _Dataset(frame)
        xarray = _XarrayStub(dataset)
        optional_module_stub({'xarray': xarray})
        path = self.format_path(tmp_path)

        result = mod.NcFile().read(path)

        assert result == [
            {'index': 2, 'value': 10},
            {'index': 4, 'value': 11},
        ]

    def test_read_raises_engine_error(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that reading raises an engine error."""

        def _open_dataset(_: Path) -> _Dataset:
            raise ImportError('engine missing')

        xarray = _XarrayStub(_Dataset(DictRecordsFrameStub([])))
        xarray.open_dataset = _open_dataset  # type: ignore[assignment]
        patch_dependency_resolver_value(
            monkeypatch,
            mod,
            value=xarray,
        )
        path = self.format_path(tmp_path)

        with pytest.raises(ImportError, match='NC support requires optional'):
            mod.NcFile().read(path)

    def test_write_happy_path(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test the happy path of writing data to a NetCDF file."""
        frame = DictRecordsFrameStub([{'value': 1}])
        dataset = _Dataset(frame)
        xarray = _XarrayStub(dataset)
        self._patch_from_dataframe(monkeypatch, xarray, dataset)
        optional_module_stub({'xarray': xarray, 'pandas': RDataPandasStub()})
        path = self.format_path(tmp_path)

        written = mod.NcFile().write(path, [{'value': 1}])

        assert written == 1
        assert dataset.to_netcdf_calls == [path]

    def test_write_raises_engine_error(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that writing raises an engine error."""
        frame = DictRecordsFrameStub([{'value': 1}])
        dataset = _Dataset(frame, fail=True)
        xarray = _XarrayStub(dataset)
        self._patch_from_dataframe(monkeypatch, xarray, dataset)
        optional_module_stub({'xarray': xarray, 'pandas': RDataPandasStub()})
        path = self.format_path(tmp_path)

        with pytest.raises(ImportError, match='NC support requires optional'):
            mod.NcFile().write(path, [{'value': 1}])

    @staticmethod
    def _patch_from_dataframe(
        monkeypatch: pytest.MonkeyPatch,
        xarray: _XarrayStub,
        dataset: _Dataset,
    ) -> None:
        """Patch xarray Dataset constructor to return a deterministic stub."""

        def _from_dataframe(_: object) -> _Dataset:
            return dataset

        monkeypatch.setattr(
            xarray.Dataset,
            'from_dataframe',
            staticmethod(_from_dataframe),
        )
