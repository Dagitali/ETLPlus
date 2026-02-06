"""
:mod:`tests.unit.file.test_u_file_parquet` module.

Unit tests for :mod:`etlplus.file.parquet`.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any
from typing import cast

import pytest

from etlplus.file import parquet as mod

# SECTION: TESTS ============================================================ #


class TestParquetRead:
    """Unit tests for :func:`etlplus.file.parquet.read`."""

    def test_read_returns_records(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
        make_records_frame: Callable[[list[dict[str, object]]], object],
        make_pandas_stub: Callable[[object], object],
    ) -> None:
        """
        Test that :func:`read` returns the records from the Parquet file.
        """
        frame = make_records_frame([{'id': 1}])
        pandas = cast(Any, make_pandas_stub(frame))
        optional_module_stub({'pandas': pandas})

        result = mod.read(tmp_path / 'data.parquet')

        assert result == [{'id': 1}]
        assert pandas.read_calls

    def test_read_wraps_import_error(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        make_import_error_reader: Callable[[str], object],
    ) -> None:
        """Test that import errors are wrapped with a helpful message."""
        monkeypatch.setattr(
            mod,
            'get_pandas',
            lambda *_: make_import_error_reader('read_parquet'),
        )

        with pytest.raises(ImportError, match='pyarrow'):
            mod.read(tmp_path / 'data.parquet')


class TestParquetWrite:
    """Unit tests for :func:`etlplus.file.parquet.write`."""

    # pylint: disable=unused-argument

    def test_write_calls_to_parquet(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
        make_records_frame: Callable[[list[dict[str, object]]], object],
        make_pandas_stub: Callable[[object], object],
    ) -> None:
        """Test that :func:`write` calls :meth:`to_parquet` on the frame."""
        frame = make_records_frame([{'id': 1}])
        pandas = cast(Any, make_pandas_stub(frame))
        optional_module_stub({'pandas': pandas})
        path = tmp_path / 'data.parquet'

        written = mod.write(path, [{'id': 1}])

        assert written == 1
        assert pandas.last_frame is not None
        assert pandas.last_frame.to_parquet_calls == [
            {'path': path, 'index': False},
        ]

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
        make_import_error_writer: Callable[[], object],
    ) -> None:
        """Test that import errors are wrapped with a helpful message."""
        monkeypatch.setattr(
            mod,
            'get_pandas',
            lambda *_: make_import_error_writer(),
        )

        with pytest.raises(ImportError, match='pyarrow'):
            mod.write(tmp_path / 'data.parquet', [{'id': 1}])
