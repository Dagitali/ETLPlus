"""
:mod:`tests.unit.file.test_u_file_xls` module.

Unit tests for :mod:`etlplus.file.xls`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from etlplus.file import xls as mod
from etlplus.file.base import ReadOptions
from tests.unit.file.pytest_file_contract_mixins import ReadOnlyWriteGuardMixin
from tests.unit.file.pytest_file_contract_mixins import (
    SpreadsheetCategoryContractBase,
)
from tests.unit.file.pytest_file_contract_mixins import (
    SpreadsheetReadImportErrorMixin,
)
from tests.unit.file.pytest_file_contract_utils import (
    patch_dependency_resolver_value,
)
from tests.unit.file.pytest_file_support import SpreadsheetSheetFrameStub
from tests.unit.file.pytest_file_support import SpreadsheetSheetPandasStub

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

    @pytest.mark.parametrize(
        ('sheet', 'read_supports_sheet_name', 'expected_read_calls'),
        [
            (
                'Sheet2',
                True,
                [{'engine': 'xlrd', 'sheet_name': 'Sheet2'}],
            ),
            (
                'Main',
                False,
                [
                    {'engine': 'xlrd', 'sheet_name': 'Main'},
                    {'engine': 'xlrd'},
                ],
            ),
        ],
        ids=['sheet_name_supported', 'sheet_name_fallback'],
    )
    def test_read_sheet_option_routing(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        sheet: str,
        read_supports_sheet_name: bool,
        expected_read_calls: list[dict[str, object]],
    ) -> None:
        """Test read sheet routing and fallback behavior."""
        pandas = SpreadsheetSheetPandasStub(
            SpreadsheetSheetFrameStub([{'id': 1}]),
            read_supports_sheet_name=read_supports_sheet_name,
        )
        patch_dependency_resolver_value(
            monkeypatch,
            mod,
            resolver_name='get_pandas',
            value=pandas,
        )
        path = self.format_path(tmp_path)

        result = mod.XlsFile().read(path, options=ReadOptions(sheet=sheet))

        assert result == [{'id': 1}]
        assert pandas.read_calls == [
            {'path': path, **call} for call in expected_read_calls
        ]
