"""
:mod:`tests.unit.file.test_u_file_xlsx` module.

Unit tests for :mod:`etlplus.file.xlsx`.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest

from etlplus.file import xlsx as mod

# SECTION: HELPERS ========================================================== #


class _Frame:
    """Minimal frame stub for Excel helpers."""

    # pylint: disable=unused-argument

    def __init__(self, records: list[dict[str, object]]) -> None:
        self._records = records
        self.to_excel_calls: list[dict[str, object]] = []

    def to_dict(
        self,
        *,
        orient: str,
    ) -> list[dict[str, object]]:  # noqa: ARG002
        """Simulate converting to a dictionary with a specific orientation."""
        return list(self._records)

    def to_excel(
        self,
        path: Path,
        *,
        index: bool,
    ) -> None:
        """Simulate writing to an Excel file by recording the call."""
        self.to_excel_calls.append({'path': path, 'index': index})


class _PandasStub:
    """Stub for pandas module."""

    def __init__(self, frame: _Frame) -> None:
        self._frame = frame
        self.read_calls: list[dict[str, object]] = []

    def read_excel(
        self,
        path: Path,
    ) -> _Frame:
        """Simulate reading an Excel file by recording the call."""
        self.read_calls.append({'path': path})
        return self._frame

    class DataFrame:  # noqa: D106
        """Simulate pandas.DataFrame with from_records method."""

        @staticmethod
        def from_records(
            records: list[dict[str, object]],
        ) -> _Frame:
            """Simulate :class:`pandas.DataFrame` with from_records method."""
            return _Frame(records)


# SECTION: TESTS ============================================================ #


class TestXlsxRead:
    """Unit tests for :func:`etlplus.file.xlsx.read`."""

    # pylint: disable=unused-argument

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
            ) -> _Frame:  # noqa: ARG002
                """Simulate failure when reading an Excel file."""
                raise ImportError('missing')

        monkeypatch.setattr(mod, 'get_pandas', lambda *_: _FailPandas())

        with pytest.raises(ImportError, match='openpyxl'):
            mod.read(tmp_path / 'data.xlsx')

    def test_read_returns_records(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """Test that :func:`read` returns the records from the Excel file."""
        frame = _Frame([{'id': 1}])
        pandas = _PandasStub(frame)
        optional_module_stub({'pandas': pandas})

        result = mod.read(tmp_path / 'data.xlsx')

        assert result == [{'id': 1}]
        assert pandas.read_calls


class TestXlsxWrite:
    """Unit tests for :func:`etlplus.file.xlsx.write`."""

    # pylint: disable=unused-argument

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
            Simulate a :class:`pandas.DataFrame` that fails when calling
            :func:`to_excel`.
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
            Simulate :mod:`pandas` module that fails on :meth:`read_excel`.
            """

            class DataFrame:  # noqa: D106
                """
                Simulate :class:`pandas.DataFrame` with :meth:`from_records`
                method.
                """

                @staticmethod
                def from_records(
                    records: list[dict[str, object]],
                ) -> _FailFrame:  # noqa: ARG002
                    """
                    Simulate :class:`pandas.DataFrame` with
                    :meth:`from_records` method.
                    """
                    return _FailFrame()

        monkeypatch.setattr(mod, 'get_pandas', lambda *_: _FailPandas())

        with pytest.raises(ImportError, match='openpyxl'):
            mod.write(tmp_path / 'data.xlsx', [{'id': 1}])

    def test_write_returns_zero_for_empty_payload(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that writing an empty payload returns zero."""
        assert mod.write(tmp_path / 'data.xlsx', []) == 0
