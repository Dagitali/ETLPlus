"""
:mod:`tests.unit.file.test_u_file_xpt` module.

Unit tests for :mod:`etlplus.file.xpt`.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest

from etlplus.file import xpt as mod
from etlplus.file.base import ReadOptions
from etlplus.file.base import WriteOptions
from tests.unit.file.conftest import SingleDatasetWritableContract

# SECTION: HELPERS ========================================================== #


class _Frame:
    """Minimal frame stub for XPT helpers."""

    # pylint: disable=unused-argument

    def __init__(self, records: list[dict[str, object]]) -> None:
        self._records = list(records)

    def to_dict(
        self,
        *,
        orient: str,  # noqa: ARG002
    ) -> list[dict[str, object]]:
        """Simulate frame-to-record conversion."""
        return list(self._records)


class _PandasStub:
    """Stub for pandas module used by XPT handlers."""

    # pylint: disable=unused-argument

    def __init__(
        self,
        frame: _Frame,
        *,
        fail_on_format_kwarg: bool = False,
    ) -> None:
        self._frame = frame
        self._fail_on_format_kwarg = fail_on_format_kwarg
        self.read_calls: list[dict[str, object]] = []

    def read_sas(
        self,
        path: Path,
        **kwargs: object,
    ) -> _Frame:
        """Simulate pandas.read_sas with optional format rejection."""
        self.read_calls.append({'path': path, **kwargs})
        if self._fail_on_format_kwarg and 'format' in kwargs:
            raise TypeError('format not supported')
        return self._frame


class _PyreadstatReadStub:
    """Stub exposing optional read_xport support."""

    # pylint: disable=unused-argument

    def __init__(self, frame: _Frame) -> None:
        self._frame = frame
        self.read_calls: list[str] = []

    def read_xport(
        self,
        path: str,
    ) -> tuple[_Frame, object]:
        """Simulate pyreadstat.read_xport behavior."""
        self.read_calls.append(path)
        return self._frame, object()


class _PyreadstatWriteStub:
    """Stub exposing write_xport support."""

    # pylint: disable=unused-argument

    def __init__(self) -> None:
        self.write_calls: list[tuple[object, str]] = []

    def write_xport(
        self,
        frame: object,
        path: str,
    ) -> None:
        """Simulate pyreadstat.write_xport behavior."""
        self.write_calls.append((frame, path))

# SECTION: TESTS ============================================================ #


class TestXpt(SingleDatasetWritableContract):
    """Unit tests for :mod:`etlplus.file.xpt`."""

    module = mod
    handler_cls = mod.XptFile
    format_name = 'xpt'

    def test_read_falls_back_to_pandas_read_sas(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """Test XPT reads falling back to pandas when read_xport is absent."""
        frame = _Frame([{'id': 1}])
        pandas = _PandasStub(frame, fail_on_format_kwarg=True)
        optional_module_stub({'pyreadstat': object(), 'pandas': pandas})

        result = mod.XptFile().read_dataset(tmp_path / 'data.xpt')

        assert result == [{'id': 1}]
        assert pandas.read_calls == [
            {'path': tmp_path / 'data.xpt', 'format': 'xport'},
            {'path': tmp_path / 'data.xpt'},
        ]

    def test_read_prefers_pyreadstat_read_xport(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """Test XPT reads preferring pyreadstat's native reader."""
        frame = _Frame([{'id': 1}])
        pyreadstat = _PyreadstatReadStub(frame)
        pandas = _PandasStub(frame)
        optional_module_stub({'pyreadstat': pyreadstat, 'pandas': pandas})

        result = mod.XptFile().read_dataset(
            tmp_path / 'data.xpt',
            options=ReadOptions(dataset='data'),
        )

        assert result == [{'id': 1}]
        assert pyreadstat.read_calls == [str(tmp_path / 'data.xpt')]
        assert pandas.read_calls == []

    def test_write_raises_when_pyreadstat_writer_missing(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """Test XPT writes requiring pyreadstat.write_xport."""
        class _PandasNoop:
            class DataFrame:  # noqa: D106
                @staticmethod
                def from_records(records: list[dict[str, object]]) -> _Frame:
                    return _Frame(records)

        optional_module_stub({'pyreadstat': object(), 'pandas': _PandasNoop()})

        with pytest.raises(ImportError, match='write_xport'):
            mod.XptFile().write_dataset(tmp_path / 'data.xpt', [{'id': 1}])

    def test_write_uses_pyreadstat_writer(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """Test XPT writes delegating to pyreadstat.write_xport."""
        frame = _Frame([])

        class _PandasWriterStub:
            class DataFrame:  # noqa: D106
                @staticmethod
                def from_records(records: list[dict[str, object]]) -> _Frame:
                    _ = records
                    return frame

        pyreadstat = _PyreadstatWriteStub()
        optional_module_stub(
            {'pyreadstat': pyreadstat, 'pandas': _PandasWriterStub()},
        )
        path = tmp_path / 'data.xpt'

        written = mod.XptFile().write_dataset(
            path,
            [{'id': 1}],
            options=WriteOptions(dataset='data'),
        )

        assert written == 1
        assert pyreadstat.write_calls
        _, write_path = pyreadstat.write_calls[-1]
        assert write_path == str(path)
