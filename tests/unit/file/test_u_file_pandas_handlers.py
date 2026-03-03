"""
:mod:`tests.unit.file.test_u_file_pandas_handlers` module.

Unit tests for :mod:`etlplus.file._pandas_handlers`.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from etlplus.file import _pandas_handlers as mod
from etlplus.file.enums import FileFormat

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


class _Handler:
    """Simple handler stub for dependency resolver tests."""


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


class _ReadExcelFallbackPandasStub:
    """
    Pandas stub that rejects ``sheet_name`` to exercise fallback behavior.
    """

    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def read_excel(self, path: object, **kwargs: object) -> str:
        """Record read_excel calls and reject ``sheet_name`` kwargs."""
        call = {'path': path, **dict(kwargs)}
        self.calls.append(call)
        if 'sheet_name' in kwargs:
            raise TypeError('sheet_name unsupported')
        return 'frame'


class _WriteExcelFallbackFrameStub:
    """Frame stub that rejects ``sheet_name`` to exercise write fallback."""

    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def to_excel(self, path: object, **kwargs: object) -> None:
        """Record to_excel calls and reject ``sheet_name`` kwargs."""
        call = {'path': path, **dict(kwargs)}
        self.calls.append(call)
        if 'sheet_name' in kwargs:
            raise TypeError('sheet_name unsupported')


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
            _Handler(),
            format_name='PARQUET',
        )

        assert result == 'fallback'
        assert calls == [('pyarrow', 'PARQUET', True)]

    def test_resolve_pyarrow_dependency_prefers_module_override(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test concrete-module ``get_pyarrow`` override path."""
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
            _Handler(),
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
        """Test unknown engine names bypassing dependency resolution."""
        monkeypatch.setattr(
            mod,
            'resolve_dependency',
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                AssertionError('resolver should not run'),
            ),
        )
        mod._resolve_spreadsheet_engine_dependency(
            _Handler(),
            engine='unknown',
            format_name='XLSX',
        )

    def test_resolve_spreadsheet_engine_dependency_uses_required_dependency(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test known engine resolution delegating with required semantics."""
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
            _Handler(),
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
        ids=['none', 'openpyxl', 'xlrd', 'odf', 'unknown'],
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

    def test_resolve_engine_uses_default_engine_when_not_overridden(
        self,
    ) -> None:
        """Test fallback engine behavior for read operations."""
        handler = _SpreadsheetEngineHandler()
        assert handler.resolve_engine('read') == 'openpyxl'

    def test_resolve_engine_prefers_write_override_for_write_operations(
        self,
    ) -> None:
        """Test write operation resolving explicit write-engine overrides."""
        handler = _SpreadsheetEngineWriteOverrideHandler()
        assert handler.resolve_engine('write') == 'odf'

    def test_resolve_read_engine_wrapper_uses_shared_engine_resolution(
        self,
    ) -> None:
        """Test read-engine wrapper forwarding to operation-aware resolver."""
        handler = _SpreadsheetReadHandler()
        assert handler.resolve_read_engine() == 'openpyxl'

    def test_resolve_write_engine_wrapper_uses_shared_engine_resolution(
        self,
    ) -> None:
        """Test write-engine wrapper forwarding to operation-aware resolver."""
        handler = _SpreadsheetWriteHandler()
        assert handler.resolve_write_engine() == 'odf'

    def test_resolve_engine_dependency_delegates_with_format_context(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test dependency enforcement wiring for resolved engines."""
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

    def test_read_excel_frame_falls_back_without_sheet_name(self) -> None:
        """Test read helper retrying when ``sheet_name`` is unsupported."""
        pandas = _ReadExcelFallbackPandasStub()
        path = Path('sample.xlsx')

        result = mod._read_excel_frame(
            pandas,
            path,
            sheet='Sheet1',
            engine='openpyxl',
        )

        assert result == 'frame'
        assert pandas.calls == [
            {
                'path': path,
                'sheet_name': 'Sheet1',
                'engine': 'openpyxl',
            },
            {'path': path, 'engine': 'openpyxl'},
        ]

    def test_read_excel_frame_without_engine_omits_engine_kwarg(self) -> None:
        """
        Test read helper not injecting engine when ``engine`` is ``None``.
        """
        pandas = _ReadExcelFallbackPandasStub()
        path = Path('sample.xlsx')

        result = mod._read_excel_frame(
            pandas,
            path,
            sheet='Sheet1',
            engine=None,
        )

        assert result == 'frame'
        assert pandas.calls == [
            {'path': path, 'sheet_name': 'Sheet1'},
            {'path': path},
        ]

    def test_write_excel_frame_falls_back_without_sheet_name(self) -> None:
        """Test write helper retrying when ``sheet_name`` is unsupported."""
        frame = _WriteExcelFallbackFrameStub()
        path = Path('sample.xlsx')

        mod._write_excel_frame(
            frame,
            path,
            sheet='Sheet1',
            engine='openpyxl',
        )

        assert frame.calls == [
            {
                'path': path,
                'index': False,
                'engine': 'openpyxl',
                'sheet_name': 'Sheet1',
            },
            {'path': path, 'index': False, 'engine': 'openpyxl'},
        ]

    def test_write_excel_frame_without_engine_omits_engine_kwarg(self) -> None:
        """
        Test write helper not injecting engine when ``engine`` is ``None``.
        """
        frame = _WriteExcelFallbackFrameStub()
        path = Path('sample.xlsx')

        mod._write_excel_frame(
            frame,
            path,
            sheet='Sheet1',
            engine=None,
        )

        assert frame.calls == [
            {'path': path, 'index': False, 'sheet_name': 'Sheet1'},
            {'path': path, 'index': False},
        ]


class TestColumnarRuntimeDependencyValidation:
    """Unit tests for columnar runtime dependency validation branches."""

    def test_validate_runtime_dependencies_noops_when_pyarrow_not_required(
        self,
    ) -> None:
        """Test runtime dependency validation when pyarrow is not required."""
        _ColumnarNoPyarrowHandler().validate_runtime_dependencies()
