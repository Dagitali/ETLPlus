"""
:mod:`tests.unit.file.test_u_file_ods` module.

Unit tests for :mod:`etlplus.file.ods`.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest

from etlplus.file import ods as mod

# SECTION: HELPERS ========================================================== #


class _Frame:
    """Minimal frame stub for ODS helpers."""

    # pylint: disable=unused-argument

    def __init__(self, records: list[dict[str, object]]) -> None:
        self._records = records
        self.to_excel_calls: list[dict[str, object]] = []

    def to_dict(
        self,
        *,
        orient: str,
    ) -> list[dict[str, object]]:  # noqa: ARG002
        """
        Simulate converting to a dictionary with a specific orientation.
        """
        return list(self._records)

    def to_excel(
        self,
        path: Path,
        *,
        index: bool,
        engine: str,
    ) -> None:
        """Simulate writing to an Excel file by recording the call."""
        self.to_excel_calls.append(
            {'path': path, 'index': index, 'engine': engine},
        )


class _PandasStub:
    """Stub for pandas module."""

    def __init__(self, frame: _Frame) -> None:
        self._frame = frame
        self.read_calls: list[dict[str, object]] = []

    def read_excel(
        self,
        path: Path,
        *,
        engine: str,
    ) -> _Frame:
        """Simulate reading an Excel file by recording the call."""
        self.read_calls.append({'path': path, 'engine': engine})
        return self._frame

    class DataFrame:  # noqa: D106
        """Simulate :class:`pandas.DataFrame` with from_records method."""

        @staticmethod
        def from_records(records: list[dict[str, object]]) -> _Frame:
            """Simulate creating a DataFrame from records."""
            return _Frame(records)


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
            ) -> _Frame:  # noqa: ARG002
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
    ) -> None:
        """Test that :func:`read` returns the records from the ODS file."""
        frame = _Frame([{'id': 1}])
        pandas = _PandasStub(frame)
        optional_module_stub({'pandas': pandas})

        result = mod.read(tmp_path / 'data.ods')

        assert result == [{'id': 1}]
        assert pandas.read_calls == [
            {'path': tmp_path / 'data.ods', 'engine': 'odf'},
        ]


class TestOdsWrite:
    """Unit tests for :func:`etlplus.file.ods.write`."""

    # pylint: disable=unused-argument

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
