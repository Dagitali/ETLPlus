"""
:mod:`tests.unit.file.test_u_file_dta` module.

Unit tests for :mod:`etlplus.file.dta`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from etlplus.file import dta as mod
from etlplus.file.base import ReadOptions
from etlplus.file.base import WriteOptions
from tests.unit.file.conftest import DictRecordsFrameStub
from tests.unit.file.conftest import OptionalModuleInstaller
from tests.unit.file.conftest import RDataPandasStub
from tests.unit.file.conftest import SingleDatasetWritableContract

# SECTION: HELPERS ========================================================== #


class _Frame(DictRecordsFrameStub):
    """Minimal frame stub for DTA helpers."""

    def __init__(self, records: list[dict[str, object]]) -> None:
        super().__init__(records)
        self.to_stata_calls: list[tuple[Path, bool]] = []

    def to_stata(
        self,
        path: Path,
        *,
        write_index: bool,
    ) -> None:
        """Simulate writing a Stata frame."""
        self.to_stata_calls.append((path, write_index))


class _PandasStub(RDataPandasStub):
    """Stub for pandas module."""

    def __init__(self, frame: _Frame) -> None:
        super().__init__()
        self._frame = frame
        self.read_calls: list[Path] = []

    def read_stata(
        self,
        path: Path,
    ) -> _Frame:
        """Simulate reading Stata data."""
        self.read_calls.append(path)
        return self._frame


# SECTION: TESTS ============================================================ #


class TestDta(SingleDatasetWritableContract):
    """Unit tests for :mod:`etlplus.file.dta`."""

    module = mod
    handler_cls = mod.DtaFile
    format_name = 'dta'

    def test_read_dataset_uses_pandas_read_stata(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test DTA reads delegating to ``pandas.read_stata``."""
        pandas = _PandasStub(_Frame([{'id': 1}]))
        optional_module_stub({'pandas': pandas, 'pyreadstat': object()})
        path = self.format_path(tmp_path)

        result = mod.DtaFile().read_dataset(
            path,
            options=ReadOptions(dataset='data'),
        )

        assert result == [{'id': 1}]
        assert pandas.read_calls == [path]

    def test_write_dataset_uses_to_stata_without_index(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test DTA writes disabling index serialization."""
        frame = _Frame([])
        pandas = _PandasStub(_Frame([]))
        monkeypatch.setattr(
            pandas.DataFrame,
            'from_records',
            staticmethod(lambda _records: frame),
        )
        optional_module_stub({'pandas': pandas, 'pyreadstat': object()})
        path = self.format_path(tmp_path)

        written = mod.DtaFile().write_dataset(
            path,
            [{'id': 1}],
            options=WriteOptions(dataset='data'),
        )

        assert written == 1
        assert frame.to_stata_calls == [(path, False)]
