"""
:mod:`tests.unit.file.test_u_file_rds` module.

Unit tests for :mod:`etlplus.file.rds`.
"""

from __future__ import annotations

from collections.abc import Callable
from collections.abc import Mapping
from pathlib import Path

from etlplus.file import rds as mod
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


class _NoWriter:
    """Stub exposing ``read_r`` without any write method."""

    # pylint: disable=unused-argument

    def read_r(
        self,
        path: str,
    ) -> dict[str, object]:  # noqa: ARG002
        """Simulate missing writer by only providing a reader."""
        return {}


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


class TestRds(RDataModuleContract):
    """Unit tests for :mod:`etlplus.file.rds`."""

    module = mod
    format_name = 'rds'
    writer_missing_pattern = 'write_rds'

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
        """Build pyreadr stub exposing ``write_rds``."""
        return _PyreadrStub(result)

    def build_reader_only_stub(self) -> _NoWriter:
        """Build pyreadr-like stub without write methods."""
        return _NoWriter()

    def assert_write_success(
        self,
        pyreadr_stub: object,
        path: Path,  # noqa: ARG002
    ) -> None:
        """Assert RDS write route using ``write_rds``."""
        stub = pyreadr_stub
        assert isinstance(stub, _PyreadrStub)
        assert stub.writes

    def test_read_dataset_data_alias_maps_single_object(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """Test dataset='data' alias selecting sole object in RDS payload."""
        pyreadr = self.build_pyreadr_stub(
            {'only': self.build_frame([{'id': 1}])},
        )
        self._install_optional_dependencies(
            optional_module_stub,
            pyreadr_stub=pyreadr,
        )

        result = mod.RdsFile().read_dataset(
            self.format_path(tmp_path),
            dataset='data',
        )

        assert result == [{'id': 1}]

    def test_read_dataset_selects_named_object(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """Test read_dataset selecting explicit object names."""
        pyreadr = self.build_pyreadr_stub(
            {
                'first': self.build_frame([{'id': 1}]),
                'second': self.build_frame([{'id': 2}]),
            },
        )
        self._install_optional_dependencies(
            optional_module_stub,
            pyreadr_stub=pyreadr,
        )

        result = mod.RdsFile().read_dataset(
            self.format_path(tmp_path),
            dataset='second',
        )

        assert result == [{'id': 2}]
