"""
:mod:`tests.unit.file.test_u_file_orc` module.

Unit tests for :mod:`etlplus.file.orc`.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from etlplus.file import orc as mod

# SECTION: HELPERS ========================================================== #


class _Frame:
    """Minimal frame stub for ORC helpers."""

    # pylint: disable=unused-argument

    def __init__(self, records: list[dict[str, object]]) -> None:
        self._records = records
        self.to_orc_calls: list[dict[str, object]] = []

    def to_dict(
        self,
        *,
        orient: str,
    ) -> list[dict[str, object]]:  # noqa: ARG002
        """Simulate converting to a dictionary with a specific orientation."""
        return list(self._records)

    def to_orc(
        self,
        path: Path,
        *,
        index: bool,
    ) -> None:
        """Simulate writing to an ORC file by recording the call."""
        self.to_orc_calls.append({'path': path, 'index': index})


class _PandasStub:
    """Stub for pandas module."""

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

    def read_orc(self, path: Path) -> _Frame:
        """Simulate reading an ORC file by recording the call."""
        self.read_calls.append({'path': path})
        return self._frame


class _PyarrowStub:
    """Stub module to satisfy ORC dependency."""


# SECTION: TESTS ============================================================ #


class TestOrcRead:
    """Unit tests for :func:`etlplus.file.orc.read`."""

    def test_read_uses_pandas(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """
        Test that :func:`read` uses the :mod:`pandas` module to read the file.
        """
        frame = _Frame([{'id': 1}])
        pandas = _PandasStub(frame)
        optional_module_stub({'pyarrow': _PyarrowStub(), 'pandas': pandas})

        result = mod.read(tmp_path / 'data.orc')

        assert result == [{'id': 1}]
        assert pandas.read_calls


class TestOrcWrite:
    """Unit tests for :func:`etlplus.file.orc.write`."""

    def test_write_calls_to_orc(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """
        Test that :func:`write` calls :meth:`to_orc` on the
        :class:`pandas.DataFrame`.
        """
        frame = _Frame([{'id': 1}])
        pandas = _PandasStub(frame)
        optional_module_stub({'pyarrow': _PyarrowStub(), 'pandas': pandas})
        path = tmp_path / 'data.orc'

        written = mod.write(path, [{'id': 1}])

        assert written == 1
        assert pandas.last_frame is not None
        assert pandas.last_frame.to_orc_calls == [
            {'path': path, 'index': False},
        ]
