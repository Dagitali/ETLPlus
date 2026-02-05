"""
:mod:`tests.unit.file.test_u_file_rda` module.

Unit tests for :mod:`etlplus.file.rda`.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest

from etlplus.file import rda as mod

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
        result: dict[str, object],
    ) -> None:
        self._result = result
        self.writes: list[tuple[str, object, dict[str, object]]] = []

    def read_r(
        self,
        path: str,
    ) -> dict[str, object]:  # noqa: ARG002
        """Simulate reading an R data file by returning the preset result."""
        return dict(self._result)

    def write_rdata(
        self,
        path: str,
        frame: object,
        **kwargs: object,
    ) -> None:
        """Simulate writing an R data file by recording the call."""
        self.writes.append((path, frame, kwargs))


class _PyreadrFallbackStub:
    """Stub exposing ``write_rda`` only."""

    # pylint: disable=unused-argument

    def __init__(self) -> None:
        self.writes: list[tuple[str, object]] = []

    def read_r(
        self,
        path: str,
    ) -> dict[str, object]:  # noqa: ARG002
        """Simulate reading an R data file by returning an empty mapping."""
        return {}

    def write_rda(
        self,
        path: str,
        frame: object,
    ) -> None:
        """Simulate writing an R data file by recording the call."""
        self.writes.append((path, frame))


# SECTION: TESTS ============================================================ #


class TestRdaRead:
    """Unit tests for :func:`etlplus.file.rda.read`."""

    def test_read_empty_result_returns_empty_list(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """Test that reading an empty result returns an empty list."""
        optional_module_stub(
            {'pyreadr': _PyreadrStub({}), 'pandas': _PandasStub()},
        )

        assert mod.read(tmp_path / 'data.rda') == []

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

        assert mod.read(tmp_path / 'data.rda') == [{'id': 1}]

    def test_read_multiple_values_returns_mapping(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """Test that reading multiple values returns a mapping."""
        result: dict[str, object] = {'one': {'id': 1}, 'two': [{'id': 2}]}
        optional_module_stub(
            {'pyreadr': _PyreadrStub(result), 'pandas': _PandasStub()},
        )

        assert mod.read(tmp_path / 'data.rda') == result


class TestRdaWrite:
    """Unit tests for :func:`etlplus.file.rda.write`."""

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

        with pytest.raises(ImportError, match='write_rdata'):
            mod.write(tmp_path / 'data.rda', [{'id': 1}])

    def test_write_prefers_write_rdata_with_df_name(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """
        Test that :func:`write` prefers :meth:`write_rdata` when available.
        """
        pyreadr = _PyreadrStub({})
        optional_module_stub({'pyreadr': pyreadr, 'pandas': _PandasStub()})
        path = tmp_path / 'data.rda'

        written = mod.write(path, [{'id': 1}])

        assert written == 1
        assert pyreadr.writes
        _, _, kwargs = pyreadr.writes[0]
        assert kwargs.get('df_name') == 'data'

    def test_write_falls_back_to_write_rda(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """
        Test that :func:`write` falls back to :meth:`write_rda` when needed.
        """
        pyreadr = _PyreadrFallbackStub()
        optional_module_stub({'pyreadr': pyreadr, 'pandas': _PandasStub()})
        path = tmp_path / 'data.rda'

        written = mod.write(path, [{'id': 1}])

        assert written == 1
        assert pyreadr.writes
