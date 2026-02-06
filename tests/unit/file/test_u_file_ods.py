"""
:mod:`tests.unit.file.test_u_file_ods` module.

Unit tests for :mod:`etlplus.file.ods`.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any
from typing import cast

import pytest

from etlplus.file import ods as mod

# SECTION: TESTS ============================================================ #


class TestOdsRead:
    """Unit tests for :func:`etlplus.file.ods.read`."""

    def test_read_wraps_import_error(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that :func:`read` wraps import errors from :mod:`pandas`."""

        class _FailPandas:
            """
            Stub for :mod:`pandas` module that fails on :meth:`read_excel`.
            """

            def read_excel(
                self,
                path: Path,
                *,
                engine: str,
            ) -> object:  # noqa: ARG002
                """
                Simulate reading an Excel file by raising :class:`ImportError`.
                """
                raise ImportError('missing')

        monkeypatch.setattr(mod, 'get_pandas', lambda *_: _FailPandas())

        with pytest.raises(ImportError, match='odfpy'):
            mod.read(tmp_path / 'data.ods')

    def test_read_returns_records(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
        make_records_frame: Callable[[list[dict[str, object]]], object],
        make_pandas_stub: Callable[[object], object],
    ) -> None:
        """Test that :func:`read` returns the records from the ODS file."""
        frame = make_records_frame([{'id': 1}])
        pandas = cast(Any, make_pandas_stub(frame))
        optional_module_stub({'pandas': pandas})

        result = mod.read(tmp_path / 'data.ods')

        assert result == [{'id': 1}]
        assert pandas.read_calls == [
            {'path': tmp_path / 'data.ods', 'engine': 'odf'},
        ]


class TestOdsWrite:
    """Unit tests for :func:`etlplus.file.ods.write`."""

    # pylint: disable=unused-argument

    def test_write_calls_to_excel_with_odf_engine(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
        make_records_frame: Callable[[list[dict[str, object]]], object],
        make_pandas_stub: Callable[[object], object],
    ) -> None:
        """Test that :func:`write` calls :meth:`to_excel` with ODF engine."""
        frame = make_records_frame([{'id': 1}])
        pandas = cast(Any, make_pandas_stub(frame))
        optional_module_stub({'pandas': pandas})
        path = tmp_path / 'data.ods'

        written = mod.write(path, [{'id': 1}])

        assert written == 1
        assert pandas.last_frame is not None
        assert pandas.last_frame.to_excel_calls == [
            {'path': path, 'index': False, 'engine': 'odf'},
        ]

    def test_write_wraps_import_error(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that :func:`write` wraps import errors from :mod:`pandas`."""

        class _FailFrame:
            """
            Stub for a :class:`pandas.DataFrame` that fails on
            :meth:`to_excel`.
            """

            def to_excel(
                self,
                path: Path,
                *,
                index: bool,
                engine: str,
            ) -> None:  # noqa: ARG002
                """
                Simulate writing to an Excel file by raising
                :class:`ImportError`.
                """
                raise ImportError('missing')

        class _FailPandas:
            """
            Stub for :mod:`pandas` module that fails on :meth:`from_records`.
            """

            class DataFrame:  # noqa: D106
                """
                Simulate :class:`pandas.DataFrame` with from_records method
                that fails.
                """

                @staticmethod
                def from_records(
                    records: list[dict[str, object]],
                ) -> _FailFrame:  # noqa: ARG002
                    """
                    Simulate creating a DataFrame from records by returning a
                    failing frame.
                    """
                    return _FailFrame()

        monkeypatch.setattr(mod, 'get_pandas', lambda *_: _FailPandas())

        with pytest.raises(ImportError, match='odfpy'):
            mod.write(tmp_path / 'data.ods', [{'id': 1}])
