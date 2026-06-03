"""
:mod:`tests.unit.file.test_u_file_pandas_handlers` module.

Unit tests for :mod:`etlplus.file._pandas_handlers`.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from etlplus.file import _pandas_handlers as mod
from etlplus.file._enums import FileFormat

from .pytest_file_support import SpreadsheetSheetFrameStub
from .pytest_file_support import SpreadsheetSheetPandasStub

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


class _SpreadsheetEngineHandler(mod._PandasSpreadsheetEngineMixin):
    """Concrete spreadsheet engine resolver stub for mixin unit tests."""

    pandas_format_name = 'XLSX'
    engine_name = 'openpyxl'


class _SpreadsheetEngineWriteOverrideHandler(_SpreadsheetEngineHandler):
    """Spreadsheet engine resolver stub with explicit write engine."""

    write_engine = 'odf'


class _SpreadsheetReadHandler(mod._PandasSpreadsheetReadMixin):
    """Concrete read-mixin stub exposing read-engine wrapper behavior."""

    pandas_format_name = 'XLSX'
    engine_name = 'openpyxl'


class _SpreadsheetWriteHandler(mod.PandasSpreadsheetHandlerMixin):
    """Concrete write-mixin stub exposing write-engine wrapper behavior."""

    format = FileFormat.XLSX
    pandas_format_name = 'XLSX'
    engine_name = 'openpyxl'
    write_engine = 'odf'

    def resolve_pandas(self) -> object:
        return object()


class _ColumnarNoPyarrowHandler(mod.PandasColumnarHandlerMixin):
    """Columnar-handler stub with pyarrow marked as not required."""

    format = FileFormat.PARQUET
    pandas_format_name = 'PARQUET'
    read_method = 'read_parquet'
    write_method = 'to_parquet'
    requires_pyarrow = False

    def resolve_pandas(self) -> object:
        return object()

    def resolve_pyarrow(self) -> object:
        raise AssertionError('resolve_pyarrow should not be called')


# SECTION: TESTS ============================================================ #


class TestResolvePyarrowDependency:
    """Unit tests for pyarrow dependency resolution helper."""

    def test_resolve_pyarrow_dependency_falls_back_to_resolve_dependency(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test fallback path when no module-level override exists."""
        monkeypatch.delattr(
            sys.modules[__name__],
            'get_pyarrow',
            raising=False,
        )
        calls: list[tuple[str, str, bool]] = []

        def _resolve(
            _handler: object,
            dependency_name: str,
            *,
            format_name: str,
            pip_name: str | None = None,
            required: bool = False,
        ) -> str:
            assert pip_name is None
            calls.append((dependency_name, format_name, required))
            return 'fallback'

        monkeypatch.setattr(mod, 'resolve_dependency', _resolve)

        result = mod._resolve_pyarrow_dependency(
            object(),
            format_name='PARQUET',
        )

        assert result == 'fallback'
        assert calls == [('pyarrow', 'PARQUET', True)]

    def test_resolve_pyarrow_dependency_prefers_module_override(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that concrete-module ``get_pyarrow`` override path."""
        sentinel = object()
        calls: list[str] = []

        def _get_pyarrow(format_name: str) -> object:
            calls.append(format_name)
            return sentinel

        monkeypatch.setattr(
            sys.modules[__name__],
            'get_pyarrow',
            _get_pyarrow,
            raising=False,
        )
        monkeypatch.setattr(
            mod,
            'resolve_dependency',
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                AssertionError('fallback should not run'),
            ),
        )

        result = mod._resolve_pyarrow_dependency(
            _SpreadsheetEngineHandler(),
            format_name='PARQUET',
        )

        assert result is sentinel
        assert calls == ['PARQUET']


class TestResolveSpreadsheetEngineDependency:
    """Unit tests for spreadsheet engine dependency resolution helper."""

    def test_resolve_spreadsheet_engine_dependency_noops_for_unknown_engine(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that unknown engine names bypass dependency resolution."""
        monkeypatch.setattr(
            mod,
            'resolve_dependency',
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                AssertionError('resolver should not run'),
            ),
        )
        mod._resolve_spreadsheet_engine_dependency(
            object(),
            engine='unknown',
            format_name='XLSX',
        )

    def test_resolve_spreadsheet_engine_dependency_uses_required_dependency(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that known engine resolution delegates with required semantics.
        """
        calls: list[tuple[str, str, str | None, bool]] = []

        def _resolve(
            _handler: object,
            dependency_name: str,
            *,
            format_name: str,
            pip_name: str | None = None,
            required: bool = False,
        ) -> object:
            calls.append((dependency_name, format_name, pip_name, required))
            return object()

        monkeypatch.setattr(mod, 'resolve_dependency', _resolve)

        mod._resolve_spreadsheet_engine_dependency(
            object(),
            engine='odf',
            format_name='ODS',
        )

        assert calls == [('odf', 'ODS', 'odfpy', True)]


class TestSpreadsheetDependencySpec:
    """Unit tests for spreadsheet dependency metadata helper."""

    @pytest.mark.parametrize(
        ('engine', 'expected'),
        [
            (None, None),
            ('openpyxl', ('openpyxl', None)),
            ('xlrd', ('xlrd', None)),
            ('odf', ('odf', 'odfpy')),
            ('unknown', None),
        ],
    )
    def test_spreadsheet_dependency_spec(
        self,
        engine: str | None,
        expected: tuple[str, str | None] | None,
    ) -> None:
        """Test spreadsheet engine metadata lookup behavior."""
        assert mod._spreadsheet_dependency_spec(engine) == expected


class TestSpreadsheetEngineResolverMixin:
    """Unit tests for shared spreadsheet engine resolver mixin behavior."""

    @pytest.mark.parametrize(
        ('handler_cls', 'method_name', 'args', 'expected'),
        [
            (_SpreadsheetEngineHandler, 'resolve_engine', ('read',), 'openpyxl'),
            (
                _SpreadsheetEngineWriteOverrideHandler,
                'resolve_engine',
                ('write',),
                'odf',
            ),
            (_SpreadsheetReadHandler, 'resolve_read_engine', (), 'openpyxl'),
            (_SpreadsheetWriteHandler, 'resolve_write_engine', (), 'odf'),
        ],
        ids=(
            'default-read-engine',
            'write-engine-override',
            'read-wrapper',
            'write-wrapper',
        ),
    )
    def test_engine_resolution_helpers_return_expected_engine(
        self,
        handler_cls: type[object],
        method_name: str,
        args: tuple[str, ...],
        expected: str,
    ) -> None:
        """Test direct and wrapper engine resolution helper behavior."""
        assert getattr(handler_cls(), method_name)(*args) == expected

    def test_resolve_engine_dependency_delegates_with_format_context(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that dependency enforcement wiring for resolved engines."""
        calls: list[tuple[str | None, str]] = []

        def _resolve(
            _handler: object,
            *,
            engine: str | None,
            format_name: str,
        ) -> None:
            calls.append((engine, format_name))

        monkeypatch.setattr(
            mod,
            '_resolve_spreadsheet_engine_dependency',
            _resolve,
        )
        handler = _SpreadsheetEngineHandler()

        resolved_engine = handler.resolve_engine_dependency('read')

        assert resolved_engine == 'openpyxl'
        assert calls == [('openpyxl', 'XLSX')]


class TestSpreadsheetReadWriteFallbacks:
    """Unit tests for spreadsheet read/write fallback helper branches."""

    @pytest.mark.parametrize(
        ('engine', 'expected_kwargs'),
        [
            (
                'openpyxl',
                [
                    {'sheet_name': 'Sheet1', 'engine': 'openpyxl'},
                    {'engine': 'openpyxl'},
                ],
            ),
            (
                None,
                [
                    {'sheet_name': 'Sheet1'},
                    {},
                ],
            ),
        ],
    )
    def test_read_excel_frame_falls_back_without_sheet_name(
        self,
        engine: str | None,
        expected_kwargs: list[dict[str, object]],
    ) -> None:
        """Test read helper retry behavior with optional engine kwargs."""
        frame = SpreadsheetSheetFrameStub([{'id': 1}])
        pandas = SpreadsheetSheetPandasStub(
            frame,
            read_supports_sheet_name=False,
        )
        path = Path('sample.xlsx')

        result = mod._read_excel_frame(
            pandas,
            path,
            sheet='Sheet1',
            engine=engine,
        )

        assert result is frame
        assert pandas.read_calls == [
            {'path': path, **kwargs} for kwargs in expected_kwargs
        ]

    @pytest.mark.parametrize(
        ('engine', 'expected_kwargs'),
        [
            (
                'openpyxl',
                [
                    {
                        'index': False,
                        'engine': 'openpyxl',
                        'sheet_name': 'Sheet1',
                    },
                    {
                        'index': False,
                        'engine': 'openpyxl',
                    },
                ],
            ),
            (
                None,
                [
                    {
                        'index': False,
                        'sheet_name': 'Sheet1',
                    },
                    {'index': False},
                ],
            ),
        ],
    )
    def test_write_excel_frame_falls_back_without_sheet_name(
        self,
        engine: str | None,
        expected_kwargs: list[dict[str, object]],
    ) -> None:
        """Test write helper retry behavior with optional engine kwargs."""
        frame = SpreadsheetSheetFrameStub([], allow_sheet_name=False)
        path = Path('sample.xlsx')

        mod._write_excel_frame(
            frame,
            path,
            sheet='Sheet1',
            engine=engine,
        )

        assert frame.to_excel_calls == [
            {'path': path, **kwargs} for kwargs in expected_kwargs
        ]


class TestColumnarRuntimeDependencyValidation:
    """Unit tests for columnar runtime dependency validation branches."""

    def test_validate_runtime_dependencies_noops_when_pyarrow_not_required(
        self,
    ) -> None:
        """
        Test runtime dependency validation when :mod:`pyarrow` is not required.
        """
        _ColumnarNoPyarrowHandler().validate_runtime_dependencies()
