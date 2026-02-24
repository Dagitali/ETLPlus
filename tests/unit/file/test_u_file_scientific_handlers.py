"""
:mod:`tests.unit.file.test_u_file_scientific_handlers` module.

Unit tests for :mod:`etlplus.file._scientific_handlers`.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from etlplus.file import _scientific_handlers as mod
from etlplus.file.enums import FileFormat

from .pytest_file_support import DictRecordsFrameStub
from .pytest_file_support import RDataPandasStub

# SECTION: HELPERS ========================================================== #


class _ScientificHandler(mod.SingleDatasetTabularScientificReadWriteMixin):
    """Concrete single-dataset scientific handler stub for mixin tests."""

    format = FileFormat.DTA
    pandas_format_name = 'DTA'

    def __init__(
        self,
        *,
        pandas_dependency: Any,
        pyreadstat_dependency: Any,
    ) -> None:
        self._pandas_dependency = pandas_dependency
        self._pyreadstat_dependency = pyreadstat_dependency
        self.resolve_pyreadstat_calls = 0
        self.read_calls: list[dict[str, Any]] = []
        self.write_calls: list[dict[str, Any]] = []

    def resolve_pandas(self) -> Any:
        """Return the injected pandas-like dependency."""
        return self._pandas_dependency

    def resolve_pyreadstat(self) -> Any:
        """Return the injected pyreadstat-like dependency."""
        self.resolve_pyreadstat_calls += 1
        return self._pyreadstat_dependency

    def read_frame(
        self,
        path: Path,
        *,
        pandas: Any,
        pyreadstat: Any | None,
        options: object | None = None,
    ) -> Any:
        """Capture read wiring and return deterministic records frame."""
        self.read_calls.append(
            {
                'path': path,
                'pandas': pandas,
                'pyreadstat': pyreadstat,
                'options': options,
            },
        )
        return DictRecordsFrameStub([{'id': 1}])

    def write_frame(
        self,
        path: Path,
        frame: Any,
        *,
        pandas: Any,
        pyreadstat: Any | None,
        options: object | None = None,
    ) -> None:
        """Capture write wiring for assertions."""
        self.write_calls.append(
            {
                'path': path,
                'frame': frame,
                'pandas': pandas,
                'pyreadstat': pyreadstat,
                'options': options,
            },
        )


class _RequiredPyreadstatScientificHandler(_ScientificHandler):
    """Scientific handler stub with required pyreadstat for read/write."""

    requires_pyreadstat_for_read = True
    requires_pyreadstat_for_write = True


# SECTION: TESTS ============================================================ #


class TestScientificHandlers:
    """Unit tests for scientific mixin dependency wiring."""

    # pylint: disable=protected-access

    def test_read_write_dataset_wiring_passes_injected_dependencies(
        self,
        tmp_path: Path,
    ) -> None:
        """Test read/write glue paths forwarding injected dependencies."""
        pandas = RDataPandasStub()
        pyreadstat = object()
        handler = _RequiredPyreadstatScientificHandler(
            pandas_dependency=pandas,
            pyreadstat_dependency=pyreadstat,
        )
        path = tmp_path / 'sample.dta'

        read_result = handler.read_dataset(path)
        written = handler.write_dataset(path, [{'id': 2}])

        assert read_result == [{'id': 1}]
        assert written == 1
        assert handler.read_calls
        assert handler.read_calls[0]['pandas'] is pandas
        assert handler.read_calls[0]['pyreadstat'] is pyreadstat
        assert handler.write_calls
        assert handler.write_calls[0]['pandas'] is pandas
        assert handler.write_calls[0]['pyreadstat'] is pyreadstat
        assert isinstance(
            handler.write_calls[0]['frame'],
            DictRecordsFrameStub,
        )

    def test_resolve_optional_pyreadstat_delegates_when_required(self) -> None:
        """Test required pyreadstat branch delegating to resolver."""
        sentinel = object()
        handler = _ScientificHandler(
            pandas_dependency=RDataPandasStub(),
            pyreadstat_dependency=sentinel,
        )

        resolved = handler._resolve_optional_pyreadstat(required=True)

        assert resolved is sentinel
        assert handler.resolve_pyreadstat_calls == 1

    def test_resolve_optional_pyreadstat_returns_none_when_not_required(
        self,
    ) -> None:
        """Test optional pyreadstat branch when dependency is not required."""
        handler = _ScientificHandler(
            pandas_dependency=RDataPandasStub(),
            pyreadstat_dependency=object(),
        )

        resolved = handler._resolve_optional_pyreadstat(required=False)

        assert resolved is None
        assert handler.resolve_pyreadstat_calls == 0
