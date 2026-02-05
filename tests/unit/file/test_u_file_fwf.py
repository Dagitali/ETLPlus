"""
:mod:`tests.unit.file.test_u_file_fwf` module.

Unit tests for :mod:`etlplus.file.fwf`.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from etlplus.file import fwf as mod

# SECTION: HELPERS ========================================================== #


class _Frame:
    """Minimal frame stub for FWF helpers."""

    # pylint: disable=unused-argument

    def __init__(self, records: list[dict[str, object]]) -> None:
        self._records = records

    def to_dict(
        self,
        *,
        orient: str,
    ) -> list[dict[str, object]]:  # noqa: ARG002
        """Simulate converting a frame to a list of records."""
        return list(self._records)


class _PandasStub:
    """Stub for pandas module."""

    def __init__(self, frame: _Frame) -> None:
        self._frame = frame
        self.read_calls: list[dict[str, object]] = []

    def read_fwf(self, path: Path) -> _Frame:
        """Simulate reading a fixed-width file into a frame."""
        self.read_calls.append({'path': path})
        return self._frame


# SECTION: TESTS ============================================================ #


class TestFwfRead:
    """Unit tests for :func:`read`."""

    def test_read_uses_pandas(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """Test that :func:`read` uses pandas to read fixed-width files."""
        frame = _Frame([{'id': 1}])
        pandas = _PandasStub(frame)
        optional_module_stub({'pandas': pandas})

        result = mod.read(tmp_path / 'data.fwf')

        assert result == [{'id': 1}]
        assert pandas.read_calls


class TestFwfWrite:
    """Unit tests for :func:`write`."""

    def test_write_empty_payload_returns_zero(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that writing an empty payload returns zero."""
        assert mod.write(tmp_path / 'data.fwf', []) == 0

    def test_write_empty_fieldnames_returns_zero(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that writing with empty fieldnames returns zero."""
        assert mod.write(tmp_path / 'data.fwf', [{}]) == 0

    def test_write_formats_fixed_width_columns(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that writing formats fixed-width columns correctly."""
        path = tmp_path / 'data.fwf'
        payload = [{'id': 1, 'name': 'Ada'}, {'id': 2, 'name': 'Bob'}]

        written = mod.write(path, payload)

        assert written == 2
        assert path.read_text(encoding='utf-8').splitlines() == [
            'id name',
            '1  Ada ',
            '2  Bob ',
        ]
