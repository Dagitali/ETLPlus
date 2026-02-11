"""
:mod:`tests.unit.file.test_u_file_rda` module.

Unit tests for :mod:`etlplus.file.rda`.
"""

from __future__ import annotations

from pathlib import Path

from etlplus.file import rda as mod
from etlplus.file.base import WriteOptions
from tests.unit.file.conftest import DictRecordsFrameStub
from tests.unit.file.conftest import OptionalModuleInstaller
from tests.unit.file.conftest import PyreadrStub
from tests.unit.file.conftest import RDataModuleContract
from tests.unit.file.conftest import RDataNoWriterStub
from tests.unit.file.conftest import RDataPandasStub

# SECTION: HELPERS ========================================================== #


class _PyreadrFallbackStub:
    """Stub exposing ``write_rda`` only."""

    # pylint: disable=unused-argument

    def __init__(self) -> None:
        self.writes: list[tuple[str, object]] = []

    def read_r(
        self,
        path: str,
    ) -> dict[str, object]:  # noqa: ARG002
        """Simulate reading an R data file by returning an empty mapping."""
        return {}

    def write_rda(
        self,
        path: str,
        frame: object,
    ) -> None:
        """Simulate writing an R data file by recording the call."""
        self.writes.append((path, frame))


# SECTION: TESTS ============================================================ #


class TestRda(RDataModuleContract):
    """Unit tests for :mod:`etlplus.file.rda`."""

    module = mod
    format_name = 'rda'
    writer_missing_pattern = 'write_rdata'

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
        """Build pyreadr stub exposing ``write_rdata``."""
        return PyreadrStub(result)

    def build_reader_only_stub(self) -> RDataNoWriterStub:
        """Build pyreadr-like stub without write methods."""
        return RDataNoWriterStub()

    def test_write_falls_back_to_write_rda(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """
        Test that :func:`write` falls back to :meth:`write_rda` when needed.
        """
        pyreadr = _PyreadrFallbackStub()
        optional_module_stub({'pyreadr': pyreadr, 'pandas': RDataPandasStub()})
        path = self.format_path(tmp_path)

        written = mod.write(path, [{'id': 1}])

        assert written == 1
        assert pyreadr.writes

    def assert_write_success(
        self,
        pyreadr_stub: object,
        path: Path,  # noqa: ARG002
    ) -> None:
        """Assert RDA write route using ``write_rdata`` with ``df_name``."""
        stub = pyreadr_stub
        assert isinstance(stub, PyreadrStub)
        assert stub.write_rdata_calls
        _, _, kwargs = stub.write_rdata_calls[0]
        assert kwargs.get('df_name') == 'data'

    def test_list_datasets_returns_default_key_for_empty_payload(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """
        Test list_datasets returning default key when file has no objects.
        """
        pyreadr = self.build_pyreadr_stub({})
        self._install_optional_dependencies(
            optional_module_stub,
            pyreadr_stub=pyreadr,
        )

        result = mod.RdaFile().list_datasets(self.format_path(tmp_path))

        assert result == ['data']

    def test_list_datasets_returns_object_names(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test list_datasets exposing keys from pyreadr payloads."""
        pyreadr = self.build_pyreadr_stub({'first': object(), 'second': 1})
        self._install_optional_dependencies(
            optional_module_stub,
            pyreadr_stub=pyreadr,
        )

        result = mod.RdaFile().list_datasets(self.format_path(tmp_path))

        assert result == ['first', 'second']

    def test_write_dataset_uses_dataset_option_name(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test write_dataset forwarding explicit dataset names to pyreadr."""
        pyreadr = self.build_pyreadr_stub({})
        self._install_optional_dependencies(
            optional_module_stub,
            pyreadr_stub=pyreadr,
        )

        written = mod.RdaFile().write_dataset(
            self.format_path(tmp_path),
            [{'id': 1}],
            options=WriteOptions(dataset='metrics'),
        )

        assert written == 1
        assert isinstance(pyreadr, PyreadrStub)
        assert pyreadr.write_rdata_calls
        _, _, kwargs = pyreadr.write_rdata_calls[-1]
        assert kwargs.get('df_name') == 'metrics'
