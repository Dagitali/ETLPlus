"""
:mod:`tests.unit.file.test_u_file_sav` module.

Unit tests for :mod:`etlplus.file.sav`.
"""

from __future__ import annotations

from pathlib import Path

from etlplus.file import sav as mod
from etlplus.file.base import ReadOptions
from etlplus.file.base import WriteOptions

from .pytest_file_contracts import SingleDatasetWritableContract
from .pytest_file_support import DictRecordsFrameStub
from .pytest_file_support import PyreadstatTabularStub
from .pytest_file_support import RDataPandasStub
from .pytest_file_types import OptionalModuleInstaller

# SECTION: TESTS ============================================================ #


class TestSav(SingleDatasetWritableContract):
    """Unit tests for :mod:`etlplus.file.sav`."""

    module = mod
    handler_cls = mod.SavFile
    format_name = 'sav'

    def test_read_dataset_uses_pyreadstat_reader(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test SAV reads delegating to ``pyreadstat.read_sav``."""
        stub = PyreadstatTabularStub(
            frame=DictRecordsFrameStub([{'id': 1}]),
            read_method_name='read_sav',
            write_method_name='write_sav',
        )
        optional_module_stub({'pyreadstat': stub})
        path = self.format_path(tmp_path)

        result = mod.SavFile().read_dataset(
            path,
            options=ReadOptions(dataset='data'),
        )

        assert result == [{'id': 1}]
        stub.assert_single_read_path(path)

    def test_write_dataset_uses_pyreadstat_writer(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test SAV writes delegating to ``pyreadstat.write_sav``."""
        stub = PyreadstatTabularStub(
            frame=DictRecordsFrameStub([]),
            read_method_name='read_sav',
            write_method_name='write_sav',
        )
        optional_module_stub({'pyreadstat': stub, 'pandas': RDataPandasStub()})
        path = self.format_path(tmp_path)

        written = mod.SavFile().write_dataset(
            path,
            [{'id': 1}],
            options=WriteOptions(dataset='data'),
        )

        assert written == 1
        stub.assert_last_write_path(path)
