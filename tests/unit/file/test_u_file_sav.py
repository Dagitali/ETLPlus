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
from tests.unit.file.conftest import PyreadstatTabularStub
from tests.unit.file.conftest import RDataPandasStub
from tests.unit.file.conftest import SingleDatasetWritableContract

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
        stub = PyreadstatTabularStub(
            frame=DictRecordsFrameStub([{'id': 1}]),
            read_method_name='read_sav',
            write_method_name='write_sav',
        )
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
        stub = PyreadstatTabularStub(
            frame=DictRecordsFrameStub([]),
            read_method_name='read_sav',
            write_method_name='write_sav',
        )
        optional_module_stub({'pyreadstat': stub, 'pandas': RDataPandasStub()})
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
