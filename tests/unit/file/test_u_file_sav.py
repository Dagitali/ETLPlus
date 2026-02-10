"""
:mod:`tests.unit.file.test_u_file_sav` module.

Unit tests for :mod:`etlplus.file.sav`.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from etlplus.file import sav as mod
from etlplus.file.base import ReadOptions
from etlplus.file.base import WriteOptions
from tests.unit.file.conftest import DictRecordsFrameStub
from tests.unit.file.conftest import SingleDatasetWritableContract

# SECTION: HELPERS ========================================================== #


class _PandasStub:
    """Stub for pandas module."""

    class DataFrame:  # noqa: D106
        @staticmethod
        def from_records(
            records: list[dict[str, object]],
        ) -> DictRecordsFrameStub:
            return DictRecordsFrameStub(records)


class _PyreadstatStub:
    """Stub for pyreadstat module."""

    # pylint: disable=unused-argument

    def __init__(self, frame: DictRecordsFrameStub) -> None:
        self._frame = frame
        self.read_calls: list[str] = []
        self.write_calls: list[tuple[object, str]] = []

    def read_sav(
        self,
        path: str,
    ) -> tuple[DictRecordsFrameStub, object]:
        """Simulate reading SAV frames."""
        self.read_calls.append(path)
        return self._frame, object()

    def write_sav(
        self,
        frame: object,
        path: str,
    ) -> None:
        """Simulate writing SAV frames."""
        self.write_calls.append((frame, path))

# SECTION: TESTS ============================================================ #


class TestSav(SingleDatasetWritableContract):
    """Unit tests for :mod:`etlplus.file.sav`."""

    module = mod
    handler_cls = mod.SavFile
    format_name = 'sav'

    def test_read_dataset_uses_pyreadstat_reader(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """Test SAV reads delegating to ``pyreadstat.read_sav``."""
        stub = _PyreadstatStub(DictRecordsFrameStub([{'id': 1}]))
        optional_module_stub({'pyreadstat': stub})

        result = mod.SavFile().read_dataset(
            tmp_path / 'data.sav',
            options=ReadOptions(dataset='data'),
        )

        assert result == [{'id': 1}]
        assert stub.read_calls == [str(tmp_path / 'data.sav')]

    def test_write_dataset_uses_pyreadstat_writer(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """Test SAV writes delegating to ``pyreadstat.write_sav``."""
        stub = _PyreadstatStub(DictRecordsFrameStub([]))
        optional_module_stub({'pyreadstat': stub, 'pandas': _PandasStub()})
        path = tmp_path / 'data.sav'

        written = mod.SavFile().write_dataset(
            path,
            [{'id': 1}],
            options=WriteOptions(dataset='data'),
        )

        assert written == 1
        assert stub.write_calls
        _, write_path = stub.write_calls[-1]
        assert write_path == str(path)
