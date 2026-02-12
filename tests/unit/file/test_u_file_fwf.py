"""
:mod:`tests.unit.file.test_u_file_fwf` module.

Unit tests for :mod:`etlplus.file.fwf`.
"""

from __future__ import annotations

from pathlib import Path

from etlplus.file import fwf as mod
from tests.unit.file.conftest import DictRecordsFrameStub
from tests.unit.file.conftest import OptionalModuleInstaller
from tests.unit.file.conftest import TextRowModuleContract

# SECTION: HELPERS ========================================================== #


class _PandasStub:
    """Stub for pandas module."""

    def __init__(self, frame: DictRecordsFrameStub) -> None:
        self._frame = frame
        self.read_calls: list[dict[str, object]] = []

    def read_fwf(self, path: Path) -> DictRecordsFrameStub:
        """Simulate reading a fixed-width file into a frame."""
        self.read_calls.append({'path': path})
        return self._frame


# SECTION: TESTS ============================================================ #


class TestFwf(TextRowModuleContract):
    """Unit tests for :mod:`etlplus.file.fwf`."""

    module = mod
    format_name = 'fwf'
    write_payload = [{'id': 1, 'name': 'Ada'}, {'id': 2, 'name': 'Bob'}]
    expected_written_count = 2
    _pandas: _PandasStub

    def assert_write_contract_result(
        self,
        path: Path,
    ) -> None:
        """Assert FWF fixed-width formatting contract output."""
        assert path.read_text(encoding='utf-8').splitlines() == [
            'id name',
            '1  Ada ',
            '2  Bob ',
        ]

    def prepare_read_case(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> tuple[Path, list[dict[str, object]]]:
        """Prepare representative FWF read input and dependency stubs."""
        frame = DictRecordsFrameStub([{'id': 1}])
        self._pandas = _PandasStub(frame)
        optional_module_stub({'pandas': self._pandas})
        path = self.format_path(tmp_path)
        return path, [{'id': 1}]

    def test_read_uses_pandas(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test that :func:`read` uses pandas to read fixed-width files."""
        path, _ = self.prepare_read_case(tmp_path, optional_module_stub)
        assert mod.FwfFile().read(path) == [{'id': 1}]
        assert self._pandas.read_calls

    def test_write_empty_fieldnames_returns_zero(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that writing with empty fieldnames returns zero."""
        assert mod.FwfFile().write(self.format_path(tmp_path), [{}]) == 0
