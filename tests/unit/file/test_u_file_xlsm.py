"""
:mod:`tests.unit.file.test_u_file_xlsm` module.

Unit tests for :mod:`etlplus.file.xlsm`.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest

from etlplus.file import xlsm as mod

# SECTION: HELPERS ========================================================== #


class _Frame:
    """Minimal frame stub for XLSM helpers."""

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

    # pylint: disable=invalid-name

    def __init__(
        self,
        frame: _Frame,
    ) -> None:
        self._frame = frame
        self.read_calls: list[dict[str, object]] = []
        self.last_frame: _Frame | None = None

        def _from_records(records: list[dict[str, object]]) -> _Frame:
            frame = _Frame(records)
            self.last_frame = frame
            return frame

        self.DataFrame = type(  # type: ignore[assignment]
            'DataFrame',
            (),
            {'from_records': staticmethod(_from_records)},
        )

    def read_excel(
        self,
        path: Path,
    ) -> _Frame:
        """Simulate reading an Excel file by recording the call."""
        self.read_calls.append({'path': path})
        return self._frame


# SECTION: TESTS ============================================================ #


class TestXlsmRead:
    """Unit tests for :func:`etlplus.file.xlsm.read`."""

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
            """
            Stub for :mod:`pandas` module that fails on :meth:`read_excel`.
            """

            def read_excel(self, path: Path) -> _Frame:  # noqa: ARG002
                """
                Simulate reading an Excel file by raising :class:`ImportError`.
                """
                raise ImportError('missing')

        monkeypatch.setattr(mod, 'get_pandas', lambda *_: _FailPandas())

        with pytest.raises(ImportError, match='openpyxl'):
            mod.read(tmp_path / 'data.xlsm')

    def test_read_returns_records(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """Test that :func:`read` returns the records from the Excel file."""
        frame = _Frame([{'id': 1}])
        pandas = _PandasStub(frame)
        optional_module_stub({'pandas': pandas})

        result = mod.read(tmp_path / 'data.xlsm')

        assert result == [{'id': 1}]
        assert pandas.read_calls


class TestXlsmWrite:
    """Unit tests for :func:`etlplus.file.xlsm.write`."""

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
            """Stub for frame that fails on :meth:`to_excel`."""

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
            Stub for :mod:`pandas` module that fails on :meth:`read_excel`.
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
                    Simulate :class:`pandas.DataFrame` with from_records method
                    that fails.
                    """
                    return _FailFrame()

        monkeypatch.setattr(mod, 'get_pandas', lambda *_: _FailPandas())

        with pytest.raises(ImportError, match='openpyxl'):
            mod.write(tmp_path / 'data.xlsm', [{'id': 1}])

    def test_write_returns_zero_for_empty_payload(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that writing an empty payload returns zero."""
        assert mod.write(tmp_path / 'data.xlsm', []) == 0

    def test_write_calls_to_excel(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """
        Test that :func:`write` calls :meth:`to_excel` on the
        :class:`pandas.DataFrame`.
        """
        frame = _Frame([{'id': 1}])
        pandas = _PandasStub(frame)
        optional_module_stub({'pandas': pandas})
        path = tmp_path / 'data.xlsm'

        written = mod.write(path, [{'id': 1}])

        assert written == 1
        assert pandas.last_frame is not None
        assert pandas.last_frame.to_excel_calls == [
            {'path': path, 'index': False},
        ]
