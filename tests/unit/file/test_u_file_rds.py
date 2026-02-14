"""
:mod:`tests.unit.file.test_u_file_rds` module.

Unit tests for :mod:`etlplus.file.rds`.
"""

from __future__ import annotations

from pathlib import Path

from etlplus.file import rds as mod

from .pytest_file_contract_contracts import RDataModuleContract
from .pytest_file_contract_mixins import OptionalModuleInstaller
from .pytest_file_support import DictRecordsFrameStub
from .pytest_file_support import PyreadrStub
from .pytest_file_support import RDataNoWriterStub
from .pytest_file_support import RDataPandasStub

# SECTION: TESTS ============================================================ #


class TestRds(RDataModuleContract):
    """Unit tests for :mod:`etlplus.file.rds`."""

    module = mod
    format_name = 'rds'
    writer_missing_pattern = 'write_rds'

    def build_frame(
        self,
        records: list[dict[str, object]],
    ) -> DictRecordsFrameStub:
        """Build a frame-like stub."""
        return DictRecordsFrameStub(records)

    def build_pandas_stub(self) -> object:
        """Build pandas stub."""
        return RDataPandasStub()

    def build_pyreadr_stub(
        self,
        result: dict[str, object],
    ) -> PyreadrStub:
        """Build pyreadr stub exposing ``write_rds``."""
        return PyreadrStub(result)

    def build_reader_only_stub(self) -> RDataNoWriterStub:
        """Build pyreadr-like stub without write methods."""
        return RDataNoWriterStub()

    def assert_write_success(
        self,
        pyreadr_stub: object,
        path: Path,  # noqa: ARG002
    ) -> None:
        """Assert RDS write route using ``write_rds``."""
        stub = pyreadr_stub
        assert isinstance(stub, PyreadrStub)
        assert stub.write_rds_calls

    def test_read_dataset_data_alias_maps_single_object(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test dataset='data' alias selecting sole object in RDS payload."""
        pyreadr = self.build_pyreadr_stub(
            {'only': self.build_frame([{'id': 1}])},
        )
        self._install_optional_dependencies(
            optional_module_stub,
            pyreadr_stub=pyreadr,
        )

        result = mod.RdsFile().read_dataset(
            self.format_path(tmp_path),
            dataset='data',
        )

        assert result == [{'id': 1}]

    def test_read_dataset_selects_named_object(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test read_dataset selecting explicit object names."""
        pyreadr = self.build_pyreadr_stub(
            {
                'first': self.build_frame([{'id': 1}]),
                'second': self.build_frame([{'id': 2}]),
            },
        )
        self._install_optional_dependencies(
            optional_module_stub,
            pyreadr_stub=pyreadr,
        )

        result = mod.RdsFile().read_dataset(
            self.format_path(tmp_path),
            dataset='second',
        )

        assert result == [{'id': 2}]
