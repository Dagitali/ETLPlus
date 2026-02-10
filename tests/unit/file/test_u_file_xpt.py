"""
:mod:`tests.unit.file.test_u_file_xpt` module.

Unit tests for :mod:`etlplus.file.xpt`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from etlplus.file import xpt as mod
from etlplus.file.base import ReadOptions
from etlplus.file.base import WriteOptions
from tests.unit.file.conftest import DictRecordsFrameStub
from tests.unit.file.conftest import OptionalModuleInstaller
from tests.unit.file.conftest import PandasReadSasStub
from tests.unit.file.conftest import PyreadstatTabularStub
from tests.unit.file.conftest import RDataPandasStub
from tests.unit.file.conftest import SingleDatasetWritableContract

# SECTION: TESTS ============================================================ #


class TestXpt(SingleDatasetWritableContract):
    """Unit tests for :mod:`etlplus.file.xpt`."""

    module = mod
    handler_cls = mod.XptFile
    format_name = 'xpt'

    def test_read_falls_back_to_pandas_read_sas(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test XPT reads falling back to pandas when read_xport is absent."""
        frame = DictRecordsFrameStub([{'id': 1}])
        pandas = PandasReadSasStub(frame, fail_on_format_kwarg=True)
        optional_module_stub({'pyreadstat': object(), 'pandas': pandas})
        path = self.format_path(tmp_path)

        result = mod.XptFile().read_dataset(path)

        assert result == [{'id': 1}]
        pandas.assert_fallback_read_calls(path, format_name='xport')

    def test_read_prefers_pyreadstat_read_xport(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test XPT reads preferring pyreadstat's native reader."""
        frame = DictRecordsFrameStub([{'id': 1}])
        pyreadstat = PyreadstatTabularStub(
            frame=frame,
            read_method_name='read_xport',
        )
        pandas = PandasReadSasStub(frame)
        optional_module_stub({'pyreadstat': pyreadstat, 'pandas': pandas})
        path = self.format_path(tmp_path)

        result = mod.XptFile().read_dataset(
            path,
            options=ReadOptions(dataset='data'),
        )

        assert result == [{'id': 1}]
        pyreadstat.assert_single_read_path(path)
        assert pandas.read_calls == []

    def test_write_raises_when_pyreadstat_writer_missing(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test XPT writes requiring pyreadstat.write_xport."""
        optional_module_stub(
            {'pyreadstat': object(), 'pandas': RDataPandasStub()},
        )
        path = self.format_path(tmp_path)

        with pytest.raises(ImportError, match='write_xport'):
            mod.XptFile().write_dataset(path, [{'id': 1}])

    def test_write_uses_pyreadstat_writer(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test XPT writes delegating to pyreadstat.write_xport."""
        pyreadstat = PyreadstatTabularStub(write_method_name='write_xport')
        optional_module_stub(
            {'pyreadstat': pyreadstat, 'pandas': RDataPandasStub()},
        )
        path = self.format_path(tmp_path)

        written = mod.XptFile().write_dataset(
            path,
            [{'id': 1}],
            options=WriteOptions(dataset='data'),
        )

        assert written == 1
        pyreadstat.assert_last_write_path(path)
