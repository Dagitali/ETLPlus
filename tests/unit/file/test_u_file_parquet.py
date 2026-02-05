"""
:mod:`tests.unit.file.test_u_file_parquet` module.

Unit tests for :mod:`etlplus.file.parquet`.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest

from etlplus.file import parquet as mod

# SECTION: HELPERS ========================================================== #


class _Frame:
    """Minimal frame stub for Parquet helpers."""

    # pylint: disable=unused-argument

    def __init__(self, records: list[dict[str, object]]) -> None:
        self._records = records
        self.to_parquet_calls: list[dict[str, object]] = []

    def to_dict(
        self,
        *,
        orient: str,
    ) -> list[dict[str, object]]:  # noqa: ARG002
        """Simulate converting to a dictionary with a specific orientation."""
        return list(self._records)

    def to_parquet(
        self,
        path: Path,
        *,
        index: bool,
    ) -> None:
        """Simulate writing to a Parquet file by recording the call."""
        self.to_parquet_calls.append({'path': path, 'index': index})


class _PandasStub:
    """Stub for pandas module."""

    # pylint: disable=invalid-name

    def __init__(self, frame: _Frame) -> None:
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

    def read_parquet(self, path: Path) -> _Frame:
        """Simulate reading a Parquet file by recording the call."""
        self.read_calls.append({'path': path})
        return self._frame


# SECTION: TESTS ============================================================ #


class TestParquetRead:
    """Unit tests for :func:`etlplus.file.parquet.read`."""

    def test_read_wraps_import_error(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that import errors are wrapped with a helpful message."""

        class _FailPandas:
            """
            Stub for :mod:`pandas` module that fails on :meth:`read_parquet`.
            """

            def read_parquet(
                self,
                path: Path,
            ) -> _Frame:  # noqa: ARG002
                """Simulate failure when reading a Parquet file."""
                raise ImportError('missing')

        monkeypatch.setattr(mod, 'get_pandas', lambda *_: _FailPandas())

        with pytest.raises(ImportError, match='pyarrow'):
            mod.read(tmp_path / 'data.parquet')

    def test_read_returns_records(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """
        Test that :func:`read` returns the records from the Parquet file.
        """
        frame = _Frame([{'id': 1}])
        pandas = _PandasStub(frame)
        optional_module_stub({'pandas': pandas})

        result = mod.read(tmp_path / 'data.parquet')

        assert result == [{'id': 1}]
        assert pandas.read_calls


class TestParquetWrite:
    """Unit tests for :func:`etlplus.file.parquet.write`."""

    # pylint: disable=unused-argument

    def test_write_returns_zero_for_empty_payload(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that writing an empty payload returns zero."""
        assert mod.write(tmp_path / 'data.parquet', []) == 0

    def test_write_wraps_import_error(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that import errors are wrapped with a helpful message."""

        class _FailFrame:
            """Stub for frame that fails on :meth:`to_parquet`."""

            def to_parquet(
                self,
                path: Path,
                *,
                index: bool,
            ) -> None:  # noqa: ARG002
                """Simulate failure when writing to a Parquet file."""
                raise ImportError('missing')

        class _FailPandas:
            """
            Stub for :mod:`pandas` module that fails when creating a
            :class:`DataFrame`.
            """

            class DataFrame:  # noqa: D106
                """
                Stub for :class:`pandas.DataFrame` that fails on
                :meth:`from_records`.
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

        with pytest.raises(ImportError, match='pyarrow'):
            mod.write(tmp_path / 'data.parquet', [{'id': 1}])

    def test_write_calls_to_parquet(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """Test that :func:`write` calls :meth:`to_parquet` on the frame."""
        frame = _Frame([{'id': 1}])
        pandas = _PandasStub(frame)
        optional_module_stub({'pandas': pandas})
        path = tmp_path / 'data.parquet'

        written = mod.write(path, [{'id': 1}])

        assert written == 1
        assert pandas.last_frame is not None
        assert pandas.last_frame.to_parquet_calls == [
            {'path': path, 'index': False},
        ]
