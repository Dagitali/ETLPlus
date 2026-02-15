"""
:mod:`tests.unit.file.pytest_file_contract_mixins` module.

Reusable contract mixins and category bases for unit tests of
:mod:`etlplus.file`.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

from etlplus.file.base import ReadOptions
from etlplus.file.base import ScientificDatasetFileHandlerABC
from etlplus.file.base import SingleDatasetScientificFileHandlerABC
from etlplus.file.base import WriteOptions
from etlplus.types import JSONData
from etlplus.utils import count_records

from ...pytest_file_common import resolve_module_handler
from .pytest_file_contract_utils import (
    assert_single_dataset_rejects_non_default_key,
)
from .pytest_file_contract_utils import make_payload
from .pytest_file_contract_utils import patch_dependency_resolver_value
from .pytest_file_support import PandasModuleStub
from .pytest_file_support import RecordsFrameStub
from .pytest_file_support import SpreadsheetSheetFrameStub
from .pytest_file_support import SpreadsheetSheetPandasStub

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'PathMixin',
    'ReadOnlyWriteGuardMixin',
    'RoundtripSpec',
    'RoundtripUnitModuleContract',
    'DelimitedReadWriteMixin',
    'DelimitedTextRowsMixin',
    'ScientificReadOnlyUnknownDatasetMixin',
    'SpreadsheetReadImportErrorMixin',
    'SemiStructuredReadMixin',
    'SemiStructuredWriteDictMixin',
    'ScientificSingleDatasetHandlerMixin',
    'SpreadsheetWritableMixin',
    'SpreadsheetSheetNameRoutingMixin',
    'DelimitedCategoryContractBase',
    'ScientificCategoryContractBase',
    'SpreadsheetCategoryContractBase',
    'SemiStructuredCategoryContractBase',
    'OptionalModuleInstaller',
]


# SECTION: TYPE ALIASES ===================================================== #


# Shared callable used by dependency-stubbing fixtures/contracts.
type OptionalModuleInstaller = Callable[[dict[str, object]], None]


# SECTION: DATA CLASSES ===================================================== #


@dataclass(frozen=True, slots=True)
class RoundtripSpec:
    """
    Declarative roundtrip case for one format-aligned unit contract.
    """

    payload: JSONData
    expected: JSONData
    stem: str = 'roundtrip'
    read_options: ReadOptions | None = None
    write_options: WriteOptions | None = None
    expected_written_count: int | None = None


# SECTION: CLASSES (PRIMARY MIXINS) ========================================= #


class PathMixin:
    """Shared path helper for format-aligned contract classes."""

    format_name: str
    module: ModuleType

    @property
    def module_handler(self) -> Any:
        """Return the module's singleton handler instance."""
        return resolve_module_handler(self.module)

    def format_path(
        self,
        tmp_path: Path,
        *,
        stem: str = 'data',
    ) -> Path:
        """Build a deterministic format-specific path."""
        return tmp_path / f'{stem}.{self.format_name}'


# SECTION: CLASSES (SECONDARY MIXINS) ======================================= #


class RoundtripUnitModuleContract(PathMixin):
    """
    Reusable unit-level write/read roundtrip contract.
    """

    # pylint: disable=unused-argument

    roundtrip_spec: RoundtripSpec

    def setup_roundtrip_dependencies(
        self,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Install optional dependencies required by a roundtrip case."""
        return None

    def normalize_roundtrip_result(
        self,
        result: JSONData,
    ) -> JSONData:
        """Normalize actual read results before assertions."""
        return result

    def normalize_roundtrip_expected(
        self,
        expected: JSONData,
    ) -> JSONData:
        """Normalize expected roundtrip payloads before assertions."""
        return expected

    def test_roundtrip_unit(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test write/read roundtrip behavior for one handler module."""
        spec = self.roundtrip_spec
        self.setup_roundtrip_dependencies(optional_module_stub)
        path = self.format_path(tmp_path, stem=spec.stem)

        written = self.module_handler.write(
            path,
            spec.payload,
            options=spec.write_options,
        )
        expected_written = spec.expected_written_count
        if expected_written is None:
            expected_written = count_records(spec.payload)
        assert written == expected_written

        result = self.module_handler.read(path, options=spec.read_options)
        assert self.normalize_roundtrip_result(result) == (
            self.normalize_roundtrip_expected(spec.expected)
        )


class EmptyWriteReturnsZeroMixin(PathMixin):
    """
    Shared mixin for contracts where empty writes should return ``0``.
    """

    assert_file_not_created_on_empty_write: bool = False

    def test_write_empty_payload_returns_zero(
        self,
        tmp_path: Path,
    ) -> None:
        """Test writing empty payloads returning zero."""
        path = self.format_path(tmp_path)
        assert self.module_handler.write(path, []) == 0
        if self.assert_file_not_created_on_empty_write:
            assert not path.exists()


class DelimitedReadWriteMixin(PathMixin):
    """
    Parametrized mixin for delimiter-forwarding read/write wrappers.
    """

    delimiter: str
    sample_rows: JSONData

    def test_read_uses_expected_delimiter(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test module read delegating with the expected delimiter."""
        calls: dict[str, object] = {}
        expected_rows: list[dict[str, object]] = [{'ok': True}]

        def _read_delimited(
            path: object,
            *,
            delimiter: str,
        ) -> list[dict[str, object]]:
            calls['path'] = path
            calls['delimiter'] = delimiter
            return expected_rows

        monkeypatch.setattr(self.module, 'read_delimited', _read_delimited)

        result = self.module_handler.read(self.format_path(tmp_path))

        assert result == expected_rows
        assert calls['delimiter'] == self.delimiter

    def test_write_uses_expected_delimiter_and_format_name(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test module write delegating with expected delimiter/format."""
        calls: dict[str, object] = {}

        def _write_delimited(
            path: object,
            data: object,
            *,
            delimiter: str,
            format_name: str,
        ) -> int:
            calls['path'] = path
            calls['data'] = data
            calls['delimiter'] = delimiter
            calls['format_name'] = format_name
            return 1

        monkeypatch.setattr(self.module, 'write_delimited', _write_delimited)

        written = self.module_handler.write(
            self.format_path(tmp_path),
            self.sample_rows,
        )

        assert written == 1
        assert calls['delimiter'] == self.delimiter
        assert calls['format_name'] == self.format_name.upper()


class DelimitedTextRowsMixin(EmptyWriteReturnsZeroMixin):
    """
    Parametrized mixin for text/fixed-width row-oriented modules.
    """

    write_payload: JSONData
    expected_written_count: int = 1

    def prepare_read_case(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> tuple[Path, JSONData]:
        """Prepare and return ``(path, expected_result)`` for read tests."""
        raise NotImplementedError

    def assert_write_contract_result(
        self,
        path: Path,
    ) -> None:
        """Assert module-specific write contract behavior."""
        assert path.exists()

    def test_read_returns_expected_rows(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test reading representative row-oriented input."""
        path, expected = self.prepare_read_case(tmp_path, optional_module_stub)

        assert self.module_handler.read(path) == expected

    def test_write_rows_contract(
        self,
        tmp_path: Path,
    ) -> None:
        """Test writing representative row payloads."""
        path = self.format_path(tmp_path)

        written = self.module_handler.write(path, self.write_payload)

        assert written == self.expected_written_count
        self.assert_write_contract_result(path)


class ScientificReadOnlyUnknownDatasetMixin(PathMixin):
    """
    Parametrized mixin for read-only scientific unknown-dataset checks.
    """

    handler_cls: type[ScientificDatasetFileHandlerABC]
    unknown_dataset_error_pattern: str

    def prepare_unknown_dataset_env(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Install stubs needed for unknown-dataset contract checks."""
        _ = tmp_path
        _ = monkeypatch
        _ = optional_module_stub

    @pytest.fixture
    def handler(self) -> ScientificDatasetFileHandlerABC:
        """Create a handler instance for read-only scientific contracts."""
        return self.handler_cls()

    def test_read_dataset_rejects_unknown_dataset(
        self,
        handler: ScientificDatasetFileHandlerABC,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test explicit unknown dataset keys being rejected."""
        self.prepare_unknown_dataset_env(
            tmp_path,
            monkeypatch,
            optional_module_stub,
        )

        with pytest.raises(
            ValueError,
            match=self.unknown_dataset_error_pattern,
        ):
            handler.read_dataset(self.format_path(tmp_path), dataset='unknown')

    def test_read_rejects_unknown_dataset_from_options(
        self,
        handler: ScientificDatasetFileHandlerABC,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test unknown dataset rejection routed via read options."""
        self.prepare_unknown_dataset_env(
            tmp_path,
            monkeypatch,
            optional_module_stub,
        )

        with pytest.raises(
            ValueError,
            match=self.unknown_dataset_error_pattern,
        ):
            handler.read(
                self.format_path(tmp_path),
                options=ReadOptions(dataset='unknown'),
            )


class ReadOnlyWriteGuardMixin(PathMixin):
    """
    Shared mixin for read-only handlers rejecting module-level writes.
    """

    read_only_error_pattern: str = 'read-only'

    def test_write_not_supported(
        self,
        tmp_path: Path,
    ) -> None:
        """Test read-only handlers rejecting writes."""
        with pytest.raises(RuntimeError, match=self.read_only_error_pattern):
            self.module_handler.write(
                self.format_path(tmp_path),
                make_payload('list'),
            )


class SpreadsheetReadImportErrorMixin(PathMixin):
    """
    Shared mixin for spreadsheet read dependency error behavior.
    """

    dependency_hint: str

    def test_read_wraps_import_error(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        make_import_error_reader: Callable[[str], object],
    ) -> None:
        """Test read wrapping dependency import errors."""
        monkeypatch.setattr(
            self.module,
            'get_pandas',
            lambda *_: make_import_error_reader('read_excel'),
        )

        with pytest.raises(ImportError, match=self.dependency_hint):
            self.module_handler.read(self.format_path(tmp_path))


class SemiStructuredReadMixin(PathMixin):
    """
    Parametrized read contract mixin for semi-structured modules.
    """

    # pylint: disable=unused-argument

    sample_read_text: str

    def assert_read_contract_result(
        self,
        result: JSONData,
    ) -> None:
        """Assert module-specific read contract expectations."""
        raise NotImplementedError

    def setup_read_dependencies(
        self,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Install optional dependencies needed for read tests."""
        raise NotImplementedError

    def test_read_parses_expected_payload(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test reading expected payload from representative text content."""
        self.setup_read_dependencies(optional_module_stub)
        path = self.format_path(tmp_path)
        path.write_text(self.sample_read_text, encoding='utf-8')

        result = self.module_handler.read(path)

        self.assert_read_contract_result(result)


class SemiStructuredWriteDictMixin(PathMixin):
    """
    Parametrized write contract mixin for semi-structured modules.
    """

    dict_payload: JSONData

    def setup_write_dependencies(
        self,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Install optional dependencies needed for write tests."""
        raise NotImplementedError

    def assert_write_contract_result(
        self,
        path: Path,
    ) -> None:
        """Assert module-specific write contract behavior."""
        raise NotImplementedError

    def test_write_accepts_single_dict_payload(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test writing a single dictionary payload."""
        self.setup_write_dependencies(optional_module_stub)
        path = self.format_path(tmp_path)

        written = self.module_handler.write(path, self.dict_payload)

        assert written == 1
        self.assert_write_contract_result(path)


class ScientificSingleDatasetHandlerMixin:
    """
    Parametrized mixin for single-dataset scientific handler behavior.
    """

    handler_cls: type[SingleDatasetScientificFileHandlerABC]
    dataset_key: str
    format_name: str

    @pytest.fixture
    def handler(self) -> SingleDatasetScientificFileHandlerABC:
        """Create a handler instance for contract tests."""
        return self.handler_cls()

    def test_uses_single_dataset_scientific_abc(
        self,
    ) -> None:
        """Test single-dataset scientific class contract."""
        assert issubclass(
            self.handler_cls,
            SingleDatasetScientificFileHandlerABC,
        )
        assert self.handler_cls.dataset_key == self.dataset_key

    def test_rejects_non_default_dataset_key(
        self,
        handler: SingleDatasetScientificFileHandlerABC,
    ) -> None:
        """Test non-default dataset keys are rejected."""
        assert_single_dataset_rejects_non_default_key(
            handler,
            suffix=self.format_name,
        )


class SpreadsheetWritableMixin(EmptyWriteReturnsZeroMixin):
    """
    Parametrized mixin for writable spreadsheet module contracts.
    """

    dependency_hint: str
    read_engine: str | None
    write_engine: str | None

    # pylint: disable=unused-argument

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
        rows: list[dict[str, object]] = [{'id': 1}]
        frame = make_records_frame(rows)
        pandas = make_pandas_stub(frame)
        optional_module_stub({'pandas': pandas})
        path = self.format_path(tmp_path)

        result = self.module_handler.read(path)

        assert result == rows
        assert pandas.read_calls
        call = pandas.read_calls[-1]
        assert call.get('path') == path
        if self.read_engine is not None:
            assert call.get('engine') == self.read_engine

    def test_write_calls_to_excel(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
        make_records_frame: Callable[
            [list[dict[str, object]]],
            RecordsFrameStub,
        ],
        make_pandas_stub: Callable[[RecordsFrameStub], PandasModuleStub],
    ) -> None:
        """Test write delegating to DataFrame.to_excel with expected args."""
        rows: list[dict[str, object]] = [{'id': 1}]
        frame = make_records_frame(rows)
        pandas = make_pandas_stub(frame)
        optional_module_stub({'pandas': pandas})
        path = self.format_path(tmp_path)

        written = self.module_handler.write(path, rows)

        assert written == 1
        assert isinstance(pandas.last_frame, RecordsFrameStub)
        frame_stub = pandas.last_frame
        assert frame_stub.to_excel_calls
        call = frame_stub.to_excel_calls[-1]
        assert call.get('path') == path
        assert call.get('index') is False
        if self.write_engine is not None:
            assert call.get('engine') == self.write_engine

    def test_write_wraps_import_error(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        make_import_error_writer: Callable[[], object],
    ) -> None:
        """Test write wrapping dependency import errors."""
        monkeypatch.setattr(
            self.module,
            'get_pandas',
            lambda *_: make_import_error_writer(),
        )

        with pytest.raises(ImportError, match=self.dependency_hint):
            self.module_handler.write(
                self.format_path(tmp_path),
                [{'id': 1}],
            )


class SpreadsheetSheetNameRoutingMixin(PathMixin):
    """
    Parametrized mixin for spreadsheet ``sheet_name`` option routing.
    """

    read_engine: str | None
    write_engine: str | None

    def _read_sheet_kwargs(self, path: Path) -> dict[str, object]:
        kwargs: dict[str, object] = {'path': path, 'sheet_name': 'Sheet2'}
        if self.read_engine is not None:
            kwargs['engine'] = self.read_engine
        return kwargs

    def _write_fallback_kwargs(self, path: Path) -> dict[str, object]:
        kwargs: dict[str, object] = {'path': path, 'index': False}
        if self.write_engine is not None:
            kwargs['engine'] = self.write_engine
        return kwargs

    def _write_sheet_kwargs(self, path: Path) -> dict[str, object]:
        kwargs: dict[str, object] = {
            'path': path,
            'index': False,
            'sheet_name': 'Sheet2',
        }
        if self.write_engine is not None:
            kwargs['engine'] = self.write_engine
        return kwargs

    def test_read_uses_sheet_name_when_supported(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test reads forwarding string sheet selectors to pandas."""
        rows: list[dict[str, object]] = [{'id': 1}]
        pandas = SpreadsheetSheetPandasStub(
            SpreadsheetSheetFrameStub(rows),
        )
        patch_dependency_resolver_value(
            monkeypatch,
            self.module,
            resolver_name='get_pandas',
            value=pandas,
        )
        path = self.format_path(tmp_path)

        result = self.module_handler.read(
            path,
            options=ReadOptions(sheet='Sheet2'),
        )

        assert result == rows
        assert pandas.read_calls == [self._read_sheet_kwargs(path)]

    def test_write_falls_back_when_sheet_name_not_supported(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test write fallback when pandas rejects ``sheet_name``."""
        rows: list[dict[str, object]] = [{'id': 1}]
        pandas = SpreadsheetSheetPandasStub(
            SpreadsheetSheetFrameStub(
                rows,
                allow_sheet_name=False,
            ),
        )
        patch_dependency_resolver_value(
            monkeypatch,
            self.module,
            resolver_name='get_pandas',
            value=pandas,
        )
        path = self.format_path(tmp_path)

        written = self.module_handler.write(
            path,
            rows,
            options=WriteOptions(sheet='Sheet2'),
        )

        assert written == 1
        assert pandas.last_frame is not None
        assert pandas.last_frame.to_excel_calls == [
            self._write_sheet_kwargs(path),
            self._write_fallback_kwargs(path),
        ]

    def test_write_uses_sheet_name_when_supported(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test writes forwarding string sheet selectors to pandas."""
        rows: list[dict[str, object]] = [{'id': 1}]
        pandas = SpreadsheetSheetPandasStub(
            SpreadsheetSheetFrameStub(rows),
        )
        patch_dependency_resolver_value(
            monkeypatch,
            self.module,
            resolver_name='get_pandas',
            value=pandas,
        )
        path = self.format_path(tmp_path)

        written = self.module_handler.write(
            path,
            rows,
            options=WriteOptions(sheet='Sheet2'),
        )

        assert written == 1
        assert pandas.last_frame is not None
        assert pandas.last_frame.to_excel_calls == [
            self._write_sheet_kwargs(path),
        ]


# SECTION: CLASSES (BASES) ================================================== #


class DelimitedCategoryContractBase(PathMixin):
    """
    Shared base contract for delimited/text category modules.
    """

    sample_rows: JSONData = [{'id': 1}]


class ScientificCategoryContractBase(PathMixin):
    """
    Shared base contract for scientific dataset handlers/modules.
    """

    dataset_key: str = 'data'


class SpreadsheetCategoryContractBase(PathMixin):
    """
    Shared base contract for spreadsheet format handlers.
    """

    dependency_hint: str
    read_engine: str | None = None
    write_engine: str | None = None


class SemiStructuredCategoryContractBase(PathMixin):
    """
    Shared base contract for semi-structured text modules.
    """

    # pylint: disable=unused-argument

    sample_read_text: str = ''
    expected_read_payload: JSONData = make_payload('dict')
    dict_payload: JSONData = make_payload('dict')

    def setup_read_dependencies(
        self,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Install optional dependencies needed for read tests."""

    def setup_write_dependencies(
        self,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Install optional dependencies needed for write tests."""

    def assert_read_contract_result(
        self,
        result: JSONData,
    ) -> None:
        """Assert module-specific read contract expectations."""
        assert result == self.expected_read_payload

    def assert_write_contract_result(
        self,
        path: Path,
    ) -> None:
        """Assert module-specific write contract expectations."""
        assert path.exists()
