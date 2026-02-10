"""
:mod:`tests.unit.file.test_u_file_xls` module.

Unit tests for :mod:`etlplus.file.xls`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from etlplus.file import xls as mod
from etlplus.file.base import ReadOptions
from tests.unit.file.conftest import DictRecordsFrameStub
from tests.unit.file.conftest import ReadOnlyWriteGuardMixin
from tests.unit.file.conftest import SpreadsheetCategoryContractBase
from tests.unit.file.conftest import SpreadsheetReadImportErrorMixin
from tests.unit.file.conftest import patch_dependency_resolver_value

# SECTION: HELPERS ========================================================== #


class _PandasStub:
    """Stub for pandas module with configurable read_excel behavior."""

    # pylint: disable=unused-argument

    def __init__(
        self,
        frame: DictRecordsFrameStub,
        *,
        fail_on_sheet_name: bool = False,
    ) -> None:
        self._frame = frame
        self._fail_on_sheet_name = fail_on_sheet_name
        self.calls: list[dict[str, object]] = []

    def read_excel(
        self,
        path: Path,
        **kwargs: object,
    ) -> DictRecordsFrameStub:
        """Simulate pandas.read_excel with optional sheet-name failure."""
        call = {'path': path, **kwargs}
        self.calls.append(call)
        if self._fail_on_sheet_name and 'sheet_name' in kwargs:
            raise TypeError('sheet_name not supported')
        return self._frame


# SECTION: TESTS ============================================================ #


class TestXls(
    SpreadsheetCategoryContractBase,
    SpreadsheetReadImportErrorMixin,
    ReadOnlyWriteGuardMixin,
):
    """Unit tests for :mod:`etlplus.file.xls`."""

    module = mod
    format_name = 'xls'
    dependency_hint = 'xlrd'

    def test_read_routes_sheet_option_to_pandas(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test read passing ``sheet`` options through to pandas."""
        frame = DictRecordsFrameStub([{'id': 1}])
        pandas = _PandasStub(frame)
        patch_dependency_resolver_value(
            monkeypatch,
            mod,
            resolver_name='get_pandas',
            value=pandas,
        )
        path = self.format_path(tmp_path)

        result = mod.XlsFile().read(path, options=ReadOptions(sheet='Sheet2'))

        assert result == [{'id': 1}]
        assert pandas.calls[-1]['sheet_name'] == 'Sheet2'
        assert pandas.calls[-1]['engine'] == 'xlrd'

    def test_read_sheet_falls_back_when_sheet_name_not_supported(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test sheet-name fallback when pandas rejects that keyword."""
        frame = DictRecordsFrameStub([{'id': 1}])
        pandas = _PandasStub(frame, fail_on_sheet_name=True)
        patch_dependency_resolver_value(
            monkeypatch,
            mod,
            resolver_name='get_pandas',
            value=pandas,
        )
        path = self.format_path(tmp_path)

        result = mod.XlsFile().read(path, options=ReadOptions(sheet='Main'))

        assert result == [{'id': 1}]
        assert pandas.calls == [
            {'path': path, 'engine': 'xlrd', 'sheet_name': 'Main'},
            {'path': path, 'engine': 'xlrd'},
        ]
