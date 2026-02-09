"""
:mod:`tests.unit.file.test_u_file_rda` module.

Unit tests for :mod:`etlplus.file.rda`.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from etlplus.file import rda as mod
from tests.unit.file.conftest import RDataModuleContract

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


class _NoWriter:
    """Stub exposing ``read_r`` without any write method."""

    # pylint: disable=unused-argument

    def read_r(
        self,
        path: str,
    ) -> dict[str, object]:  # noqa: ARG002
        """Simulate missing writer by only providing a reader."""
        return {}


# SECTION: TESTS ============================================================ #


class TestRda(RDataModuleContract):
    """Unit tests for :mod:`etlplus.file.rda`."""

    module = mod
    format_name = 'rda'
    writer_missing_pattern = 'write_rdata'

    def build_frame(
        self,
        records: list[dict[str, object]],
    ) -> _Frame:
        """Build a frame-like stub."""
        return _Frame(records)

    def build_pandas_stub(self) -> _PandasStub:
        """Build pandas stub."""
        return _PandasStub()

    def build_pyreadr_stub(
        self,
        result: dict[str, object],
    ) -> _PyreadrStub:
        """Build pyreadr stub exposing ``write_rdata``."""
        return _PyreadrStub(result)

    def build_reader_only_stub(self) -> _NoWriter:
        """Build pyreadr-like stub without write methods."""
        return _NoWriter()

    def assert_write_success(
        self,
        pyreadr_stub: object,
        path: Path,  # noqa: ARG002
    ) -> None:
        """Assert RDA write route using ``write_rdata`` with ``df_name``."""
        stub = pyreadr_stub
        assert isinstance(stub, _PyreadrStub)
        assert stub.writes
        _, _, kwargs = stub.writes[0]
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
