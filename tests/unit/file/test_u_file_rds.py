"""
:mod:`tests.unit.file.test_u_file_rds` module.

Unit tests for :mod:`etlplus.file.rds`.
"""

from __future__ import annotations

from collections.abc import Callable
from collections.abc import Mapping
from pathlib import Path

import pytest

from etlplus.file import rds as mod

# SECTION: HELPERS ========================================================== #


class _Frame:
    """Minimal frame stub."""

    # pylint: disable=unused-argument

    def __init__(self, records: list[dict[str, object]]) -> None:
        self._records = records

    def to_dict(
        self,
        *,
        orient: str,
    ) -> list[dict[str, object]]:  # noqa: ARG002
        """Simulate converting to a dictionary with a specific orientation."""
        return list(self._records)

    @staticmethod
    def from_records(
        records: list[dict[str, object]],
    ) -> _Frame:
        """Simulate pandas.DataFrame.from_records."""
        return _Frame(records)


class _PandasStub:
    """Stub for :mod:`pandas` module."""

    DataFrame = _Frame


class _PyreadrStub:
    """Stub for pyreadr module."""

    # pylint: disable=unused-argument

    def __init__(
        self,
        result: Mapping[str, object],
    ) -> None:
        self._result = result
        self.writes: list[tuple[str, object]] = []

    def read_r(
        self,
        path: str,
    ) -> dict[str, object]:  # noqa: ARG002
        """Simulate reading an RDS file."""
        return dict(self._result)

    def write_rds(
        self,
        path: str,
        frame: object,
    ) -> None:
        """Simulate writing an RDS file by recording the call."""
        self.writes.append((path, frame))


# SECTION: TESTS ============================================================ #


class TestRdsRead:
    """Unit tests for :func:`etlplus.file.rds.read`."""

    def test_read_empty_result_returns_empty_list(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """Test that reading an empty result returns an empty list."""
        optional_module_stub(
            {'pyreadr': _PyreadrStub({}), 'pandas': _PandasStub()},
        )

        assert mod.read(tmp_path / 'data.rds') == []

    def test_read_single_value_coerces(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """Test that reading a single value coerces it to a list."""
        frame = _Frame([{'id': 1}])
        optional_module_stub(
            {
                'pyreadr': _PyreadrStub({'data': frame}),
                'pandas': _PandasStub(),
            },
        )

        assert mod.read(tmp_path / 'data.rds') == [{'id': 1}]

    def test_read_multiple_values_returns_mapping(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """Test that reading multiple values returns a mapping."""
        result = {'one': {'id': 1}, 'two': [{'id': 2}]}
        optional_module_stub(
            {'pyreadr': _PyreadrStub(result), 'pandas': _PandasStub()},
        )

        assert mod.read(tmp_path / 'data.rds') == result


class TestRdsWrite:
    """Unit tests for :func:`etlplus.file.rds.write`."""

    # pylint: disable=unused-argument

    def test_write_raises_when_writer_missing(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """
        Test that :func:`write` raises an error when no writer is available.
        """

        class _NoWriter:  # noqa: D401
            def read_r(self, path: str) -> dict[str, object]:  # noqa: ARG002
                """Simulate missing writer by only providing a reader."""
                return {}

        optional_module_stub({'pyreadr': _NoWriter(), 'pandas': _PandasStub()})

        with pytest.raises(ImportError, match='write_rds'):
            mod.write(tmp_path / 'data.rds', [{'id': 1}])

    def test_write_uses_write_rds(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """Test that :func:`write` uses :meth:`write_rds` when available."""
        pyreadr = _PyreadrStub({})
        optional_module_stub({'pyreadr': pyreadr, 'pandas': _PandasStub()})
        path = tmp_path / 'data.rds'

        written = mod.write(path, [{'id': 1}])

        assert written == 1
        assert pyreadr.writes
