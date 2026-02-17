"""
:mod:`tests.unit.file.pytest_file_contracts_tabular` module.

Delimited/text/spreadsheet/table contract suites for unit tests of
:mod:`etlplus.file`.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import cast

import pytest

from .pytest_file_contract_bases import DelimitedCategoryContractBase
from .pytest_file_contract_bases import SpreadsheetCategoryContractBase
from .pytest_file_contract_mixins import DelimitedReadWriteMixin
from .pytest_file_contract_mixins import DelimitedTextRowsMixin
from .pytest_file_contract_mixins import EmptyWriteReturnsZeroMixin
from .pytest_file_contract_mixins import PathMixin
from .pytest_file_contract_mixins import SpreadsheetReadImportErrorMixin
from .pytest_file_contract_mixins import SpreadsheetSheetNameRoutingMixin
from .pytest_file_contract_mixins import SpreadsheetWritableMixin
from .pytest_file_contract_utils import Operation
from .pytest_file_contract_utils import (
    call_module_operation as _call_module_operation,
)
from .pytest_file_contract_utils import make_payload
from .pytest_file_support import PandasModuleStub
from .pytest_file_support import RecordsFrameStub
from .pytest_file_types import OptionalModuleInstaller

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'DelimitedModuleContract',
    'EmbeddedDatabaseModuleContract',
    'PandasColumnarModuleContract',
    'PyarrowGatedPandasColumnarModuleContract',
    'PyarrowMissingDependencyMixin',
    'TextRowModuleContract',
    'WritableSpreadsheetModuleContract',
]


# SECTION: CLASSES ========================================================== #


class DelimitedModuleContract(
    DelimitedCategoryContractBase,
    DelimitedReadWriteMixin,
):
    """Reusable contract suite for standard delimited wrapper modules."""


class EmbeddedDatabaseModuleContract(EmptyWriteReturnsZeroMixin):
    """Reusable contract suite for embedded database wrapper modules."""

    # pylint: disable=unused-argument

    multi_table_error_pattern: str

    def build_empty_database_path(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> Path:
        """Create an empty database fixture path for read tests."""
        raise NotImplementedError

    def build_multi_table_database_path(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> Path:
        """Create a multi-table database fixture path for read tests."""
        raise NotImplementedError

    def test_read_returns_empty_when_no_tables(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test reading empty embedded databases returning no records."""
        path = self.build_empty_database_path(tmp_path, optional_module_stub)
        assert self.module_handler.read(path) == []

    def test_read_raises_on_multiple_tables(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test read rejecting ambiguous multi-table databases."""
        path = self.build_multi_table_database_path(
            tmp_path,
            optional_module_stub,
        )
        with pytest.raises(ValueError, match=self.multi_table_error_pattern):
            self.module_handler.read(path)


class PandasColumnarModuleContract(EmptyWriteReturnsZeroMixin):
    """Reusable contract suite for pandas-backed columnar format modules."""

    read_method_name: str
    write_calls_attr: str
    write_uses_index: bool = False
    requires_pyarrow: bool = False
    read_error_pattern: str = 'missing'
    write_error_pattern: str = 'missing'

    def _install_dependencies(
        self,
        optional_module_stub: OptionalModuleInstaller,
        *,
        pandas: object | None = None,
    ) -> None:
        """Install optional stubs required by columnar contract tests."""
        mapping: dict[str, object] = {}
        if pandas is not None:
            mapping['pandas'] = pandas
        if self.requires_pyarrow:
            mapping['pyarrow'] = object()
        if mapping:
            optional_module_stub(mapping)

    def test_read_returns_records(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
        make_records_frame: Callable[
            [list[dict[str, object]]],
            RecordsFrameStub,
        ],
        make_pandas_stub: Callable[[RecordsFrameStub], PandasModuleStub],
    ) -> None:
        """Test read returning row records via pandas."""
        frame = make_records_frame([{'id': 1}])
        pandas = make_pandas_stub(frame)
        self._install_dependencies(optional_module_stub, pandas=pandas)

        result = self.module_handler.read(self.format_path(tmp_path))

        assert result == make_payload('list')
        assert pandas.read_calls

    def test_write_calls_expected_table_writer(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
        make_records_frame: Callable[
            [list[dict[str, object]]],
            RecordsFrameStub,
        ],
        make_pandas_stub: Callable[[RecordsFrameStub], PandasModuleStub],
    ) -> None:
        """Test write calling the expected DataFrame writer method."""
        frame = make_records_frame([{'id': 1}])
        pandas = make_pandas_stub(frame)
        self._install_dependencies(optional_module_stub, pandas=pandas)
        path = self.format_path(tmp_path)

        written = self.module_handler.write(path, make_payload('list'))

        assert written == 1
        assert pandas.last_frame is not None
        calls = cast(
            list[dict[str, object]],
            getattr(pandas.last_frame, self.write_calls_attr),
        )
        assert calls
        call = calls[-1]
        assert call.get('path') == path
        if self.write_uses_index:
            assert call.get('index') is False
        else:
            assert 'index' not in call

    def test_read_import_error_path(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        make_import_error_reader: Callable[[str], object],
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test read dependency failures raising :class:`ImportError`."""
        self._install_dependencies(optional_module_stub)
        monkeypatch.setattr(
            self.module,
            'get_pandas',
            lambda *_: make_import_error_reader(self.read_method_name),
        )

        with pytest.raises(ImportError, match=self.read_error_pattern):
            self.module_handler.read(self.format_path(tmp_path))

    def test_write_import_error_path(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        make_import_error_writer: Callable[[], object],
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test write dependency failures raising :class:`ImportError`."""
        self._install_dependencies(optional_module_stub)
        monkeypatch.setattr(
            self.module,
            'get_pandas',
            lambda *_: make_import_error_writer(),
        )

        with pytest.raises(ImportError, match=self.write_error_pattern):
            self.module_handler.write(
                self.format_path(tmp_path),
                make_payload('list'),
            )


class PyarrowMissingDependencyMixin(PathMixin):
    """
    Shared mixin for pyarrow-gated read/write dependency checks.
    """

    missing_dependency_pattern: str = 'missing pyarrow'

    @pytest.mark.parametrize('operation', ['read', 'write'])
    def test_operations_raise_when_pyarrow_missing(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        operation: Operation,
    ) -> None:
        """Test read/write failing when pyarrow dependency resolution fails."""

        def _missing(*_args: object, **_kwargs: object) -> object:
            raise ImportError(self.missing_dependency_pattern)

        monkeypatch.setattr(self.module, 'get_dependency', _missing)
        path = self.format_path(tmp_path)

        with pytest.raises(ImportError, match=self.missing_dependency_pattern):
            _call_module_operation(
                self.module,
                operation=operation,
                path=path,
            )


class PyarrowGatedPandasColumnarModuleContract(
    PyarrowMissingDependencyMixin,
    PandasColumnarModuleContract,
):
    """
    Reusable suite for pandas-backed columnar modules gated by pyarrow.
    """

    requires_pyarrow = True
    missing_dependency_pattern: str = 'missing pyarrow'


class TextRowModuleContract(
    DelimitedCategoryContractBase,
    DelimitedTextRowsMixin,
):
    """
    Reusable contract suite for text/fixed-width row-oriented modules.
    """


class WritableSpreadsheetModuleContract(
    SpreadsheetCategoryContractBase,
    SpreadsheetReadImportErrorMixin,
    SpreadsheetSheetNameRoutingMixin,
    SpreadsheetWritableMixin,
):
    """
    Reusable contract suite for writable spreadsheet wrapper modules.
    """
