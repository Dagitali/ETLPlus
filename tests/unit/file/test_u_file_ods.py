"""
:mod:`tests.unit.file.test_u_file_ods` module.

Unit tests for :mod:`etlplus.file.ods`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from etlplus.file import ods as mod
from etlplus.file.base import ReadOptions
from etlplus.file.base import WriteOptions
from tests.unit.file.conftest import DictRecordsFrameStub
from tests.unit.file.conftest import WritableSpreadsheetModuleContract
from tests.unit.file.conftest import patch_dependency_resolver_value

# SECTION: HELPERS ========================================================== #


class _FrameStub(DictRecordsFrameStub):
    """Frame stub with optional support for ``sheet_name`` on writes."""

    def __init__(
        self,
        records: list[dict[str, object]],
        *,
        allow_sheet_name: bool = True,
    ) -> None:
        super().__init__(records)
        self.allow_sheet_name = allow_sheet_name
        self.to_excel_calls: list[dict[str, object]] = []

    def to_excel(
        self,
        path: Path,
        **kwargs: object,
    ) -> None:
        """Record to_excel writes with optional ``sheet_name`` rejection."""
        self.to_excel_calls.append({'path': path, **kwargs})
        if not self.allow_sheet_name and 'sheet_name' in kwargs:
            raise TypeError('sheet_name not supported')


class _PandasStub:
    """Pandas-like stub with configurable sheet-name support."""

    # pylint: disable=invalid-name, unused-argument

    def __init__(
        self,
        frame: _FrameStub,
        *,
        read_supports_sheet_name: bool = True,
    ) -> None:
        self.frame = frame
        self.read_supports_sheet_name = read_supports_sheet_name
        self.read_calls: list[dict[str, object]] = []
        self.last_frame: _FrameStub | None = None
        self.DataFrame = type(
            'DataFrame',
            (),
            {'from_records': staticmethod(self._from_records)},
        )

    def _from_records(
        self,
        records: list[dict[str, object]],
    ) -> _FrameStub:
        created = _FrameStub(
            records,
            allow_sheet_name=self.frame.allow_sheet_name,
        )
        self.last_frame = created
        return created

    def read_excel(
        self,
        path: Path,
        **kwargs: object,
    ) -> _FrameStub:
        """Simulate pandas.read_excel with optional sheet-name rejection."""
        self.read_calls.append({'path': path, **kwargs})
        if not self.read_supports_sheet_name and 'sheet_name' in kwargs:
            raise TypeError('sheet_name not supported')
        return self.frame


# SECTION: TESTS ============================================================ #


class TestOds(WritableSpreadsheetModuleContract):
    """Unit tests for :mod:`etlplus.file.ods`."""

    module = mod
    format_name = 'ods'
    dependency_hint = 'odfpy'
    read_engine = 'odf'
    write_engine = 'odf'

    def test_read_uses_sheet_name_when_supported(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test read forwarding string sheet selectors to pandas."""
        pandas = _PandasStub(_FrameStub([{'id': 1}]))
        patch_dependency_resolver_value(
            monkeypatch,
            mod,
            resolver_name='get_pandas',
            value=pandas,
        )
        path = self.format_path(tmp_path)

        result = mod.OdsFile().read(
            path,
            options=ReadOptions(sheet='Sheet2'),
        )

        assert result == [{'id': 1}]
        assert pandas.read_calls == [
            {'path': path, 'engine': 'odf', 'sheet_name': 'Sheet2'},
        ]

    def test_write_falls_back_when_sheet_name_not_supported(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test sheet-name fallback when pandas rejects that keyword."""
        pandas = _PandasStub(_FrameStub([{'id': 1}], allow_sheet_name=False))
        patch_dependency_resolver_value(
            monkeypatch,
            mod,
            resolver_name='get_pandas',
            value=pandas,
        )
        path = self.format_path(tmp_path)

        written = mod.OdsFile().write(
            path,
            [{'id': 1}],
            options=WriteOptions(sheet='Sheet2'),
        )

        assert written == 1
        assert pandas.last_frame is not None
        assert pandas.last_frame.to_excel_calls == [
            {
                'path': path,
                'index': False,
                'engine': 'odf',
                'sheet_name': 'Sheet2',
            },
            {'path': path, 'index': False, 'engine': 'odf'},
        ]

    def test_write_uses_sheet_name_when_supported(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test write forwarding string sheet selectors to pandas."""
        pandas = _PandasStub(_FrameStub([{'id': 1}], allow_sheet_name=True))
        patch_dependency_resolver_value(
            monkeypatch,
            mod,
            resolver_name='get_pandas',
            value=pandas,
        )
        path = self.format_path(tmp_path)

        written = mod.OdsFile().write(
            path,
            [{'id': 1}],
            options=WriteOptions(sheet='Sheet2'),
        )

        assert written == 1
        assert pandas.last_frame is not None
        assert pandas.last_frame.to_excel_calls == [
            {
                'path': path,
                'index': False,
                'engine': 'odf',
                'sheet_name': 'Sheet2',
            },
        ]
