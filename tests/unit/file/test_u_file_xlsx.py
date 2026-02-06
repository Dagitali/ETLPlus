"""
:mod:`tests.unit.file.test_u_file_xlsx` module.

Unit tests for :mod:`etlplus.file.xlsx`.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any
from typing import cast

import pytest

from etlplus.file import xlsx as mod

# SECTION: TESTS ============================================================ #


class TestXlsxRead:
    """Unit tests for :func:`etlplus.file.xlsx.read`."""

    # pylint: disable=unused-argument

    def test_read_returns_records(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
        make_records_frame: Callable[[list[dict[str, object]]], object],
        make_pandas_stub: Callable[[object], object],
    ) -> None:
        """Test that :func:`read` returns the records from the Excel file."""
        frame = make_records_frame([{'id': 1}])
        pandas = cast(Any, make_pandas_stub(frame))
        optional_module_stub({'pandas': pandas})

        result = mod.read(tmp_path / 'data.xlsx')

        assert result == [{'id': 1}]
        assert pandas.read_calls

    def test_read_wraps_import_error(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that :func:`read` wraps :class:`ImportError` from :mod:`pandas`.
        """

        class _FailPandas:
            """Simulate failure when reading an Excel file."""

            def read_excel(
                self,
                path: Path,
            ) -> object:  # noqa: ARG002
                """Simulate failure when reading an Excel file."""
                raise ImportError('missing')

        monkeypatch.setattr(mod, 'get_pandas', lambda *_: _FailPandas())

        with pytest.raises(ImportError, match='openpyxl'):
            mod.read(tmp_path / 'data.xlsx')


class TestXlsxWrite:
    """Unit tests for :func:`etlplus.file.xlsx.write`."""

    # pylint: disable=unused-argument

    def test_write_calls_to_excel(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
        make_records_frame: Callable[[list[dict[str, object]]], object],
        make_pandas_stub: Callable[[object], object],
    ) -> None:
        """Test that :func:`write` calls :meth:`to_excel` on the frame."""
        frame = make_records_frame([{'id': 1}])
        pandas = cast(Any, make_pandas_stub(frame))
        optional_module_stub({'pandas': pandas})
        path = tmp_path / 'data.xlsx'

        written = mod.write(path, [{'id': 1}])

        assert written == 1
        assert pandas.last_frame is not None
        assert pandas.last_frame.to_excel_calls == [
            {'path': path, 'index': False},
        ]

    def test_write_returns_zero_for_empty_payload(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that writing an empty payload returns zero."""
        assert mod.write(tmp_path / 'data.xlsx', []) == 0

    def test_write_wraps_import_error(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that :func:`write` wraps :class:`ImportError` from :mod:`pandas`.
        """

        class _FailFrame:
            """
            Stub for :class:`pandas.DataFrame` that fails on :meth:`to_excel`.
            """

            def to_excel(
                self,
                path: Path,
                *,
                index: bool,
            ) -> None:  # noqa: ARG002
                """Simulate failure when writing to an Excel file."""
                raise ImportError('missing')

        class _FailPandas:
            """
            Stub for :mod:`pandas` module that fails creating
            :class:`pandas.DataFrame`.
            """

            class DataFrame:  # noqa: D106
                """
                Stub for :class:`pandas.DataFrame` that fails on
                :meth:`to_excel`.
                """

                @staticmethod
                def from_records(
                    records: list[dict[str, object]],
                ) -> _FailFrame:  # noqa: ARG002
                    """
                    Simulate :class:`pandas.DataFrame` with
                    :meth:`from_records` method that fails.
                    """
                    return _FailFrame()

        monkeypatch.setattr(mod, 'get_pandas', lambda *_: _FailPandas())

        with pytest.raises(ImportError, match='openpyxl'):
            mod.write(tmp_path / 'data.xlsx', [{'id': 1}])
