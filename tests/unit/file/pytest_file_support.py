"""
:mod:`tests.unit.file.pytest_file_support` module.

Shared unit-test stubs and helper factories for :mod:`etlplus.file` tests.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

# SECTION: FUNCTIONS ======================================================== #


def make_import_error_reader_module(
    method_name: str,
) -> object:
    """Build a module-like object whose reader method raises ImportError."""

    def _fail_reader(
        *args: object,
        **kwargs: object,
    ) -> object:  # noqa: ARG001
        raise ImportError('missing')

    return SimpleNamespace(**{method_name: _fail_reader})


def make_import_error_writer_module() -> object:
    """Build a pandas-like module whose DataFrame writes raise ImportError."""

    # pylint: disable=unused-argument

    class _FailFrame:
        """Frame stub whose write-like attributes raise ImportError."""

        def __getattr__(self, name: str) -> object:
            if not name.startswith('to_'):
                raise AttributeError(name)

            def _fail_writer(
                *args: object,
                **kwargs: object,
            ) -> None:  # noqa: ARG001
                raise ImportError('missing')

            return _fail_writer

    class _DataFrame:
        """Minimal DataFrame namespace for write-path tests."""

        @staticmethod
        def from_records(
            records: list[dict[str, object]],
        ) -> _FailFrame:  # noqa: ARG002
            return _FailFrame()

    return SimpleNamespace(DataFrame=_DataFrame)


# SECTION: CLASSES ========================================================== #


class DictRecordsFrameStub:
    """
    Minimal records-only frame stub shared by scientific format tests.
    """

    # pylint: disable=unused-argument

    def __init__(
        self,
        records: list[dict[str, object]],
    ) -> None:
        self._records = list(records)
        self.columns = list(records[0].keys()) if records else []

    def __getitem__(self, key: str) -> list[object]:
        """Return one column as a list of row values."""
        return [row.get(key) for row in self._records]

    def drop(
        self,
        columns: list[str],
    ) -> DictRecordsFrameStub:
        """Return a new frame with selected columns removed."""
        return DictRecordsFrameStub(
            [
                {k: v for k, v in row.items() if k not in columns}
                for row in self._records
            ],
        )

    def reset_index(self) -> DictRecordsFrameStub:
        """Return self for simple reset-index test flows."""
        return self

    def to_dict(
        self,
        *,
        orient: str,
    ) -> list[dict[str, object]]:  # noqa: ARG002
        """Return record payloads in ``records`` orientation."""
        return list(self._records)

    @staticmethod
    def from_records(
        records: list[dict[str, object]],
    ) -> DictRecordsFrameStub:
        """Construct a frame from row records."""
        return DictRecordsFrameStub(records)


class PandasModuleStub:
    """Minimal pandas-module stub with reader and DataFrame helpers."""

    # pylint: disable=invalid-name

    def __init__(
        self,
        frame: RecordsFrameStub,
    ) -> None:
        self._frame = frame
        self.read_calls: list[dict[str, object]] = []
        self.last_frame: RecordsFrameStub | None = None

        def _from_records(
            records: list[dict[str, object]],
        ) -> RecordsFrameStub:
            created = RecordsFrameStub(records)
            self.last_frame = created
            return created

        self.DataFrame = type(
            'DataFrame',
            (),
            {'from_records': staticmethod(_from_records)},
        )

    def _record_read(
        self,
        path: Path,
        **kwargs: object,
    ) -> RecordsFrameStub:
        call: dict[str, object] = {'path': path, **kwargs}
        self.read_calls.append(call)
        return self._frame

    def read_excel(
        self,
        path: Path,
        *,
        engine: str | None = None,
    ) -> RecordsFrameStub:
        """Simulate ``pandas.read_excel``."""
        if engine is None:
            return self._record_read(path)
        return self._record_read(path, engine=engine)

    def _read_table(
        self,
        path: Path,
    ) -> RecordsFrameStub:
        """Simulate table-like pandas readers returning record frames."""
        return self._record_read(path)

    read_parquet = _read_table
    read_feather = _read_table
    read_orc = _read_table


class PandasReadSasStub:
    """
    Minimal pandas stub for ``read_sas``-based handlers.
    """

    def __init__(
        self,
        frame: DictRecordsFrameStub,
        *,
        fail_on_format_kwarg: bool = False,
    ) -> None:
        self._frame = frame
        self._fail_on_format_kwarg = fail_on_format_kwarg
        self.read_calls: list[dict[str, object]] = []

    def assert_fallback_read_calls(
        self,
        path: Path,
        *,
        format_name: str,
    ) -> None:
        """Assert fallback behavior after a rejected format keyword read."""
        assert self.read_calls == [
            {'path': path, 'format': format_name},
            {'path': path},
        ]

    def assert_single_read_call(
        self,
        path: Path,
        *,
        format_name: str | None = None,
    ) -> None:
        """
        Assert one pandas :meth:`read_sas` call with optional format hint.
        """
        expected: dict[str, object] = {'path': path}
        if format_name is not None:
            expected['format'] = format_name
        assert self.read_calls == [expected]

    def read_sas(
        self,
        path: Path,
        **kwargs: object,
    ) -> DictRecordsFrameStub:
        """Simulate pandas.read_sas with optional format rejection."""
        self.read_calls.append({'path': path, **kwargs})
        if self._fail_on_format_kwarg and 'format' in kwargs:
            raise TypeError('format not supported')
        return self._frame


class PyreadrStub:
    """
    Shared pyreadr-style stub for RDA/RDS tests.
    """

    # pylint: disable=unused-argument

    def __init__(
        self,
        result: dict[str, object],
    ) -> None:
        self._result = result
        self.write_rds_calls: list[tuple[str, object]] = []
        self.write_rdata_calls: list[
            tuple[str, object, dict[str, object]]
        ] = []

    def read_r(
        self,
        path: str,
    ) -> dict[str, object]:
        """Return configured R object mapping."""
        return dict(self._result)

    def write_rds(
        self,
        path: str,
        frame: object,
    ) -> None:
        """Record one RDS write call."""
        self.write_rds_calls.append((path, frame))

    def write_rdata(
        self,
        path: str,
        frame: object,
        **kwargs: object,
    ) -> None:
        """Record one RDA write call."""
        self.write_rdata_calls.append((path, frame, dict(kwargs)))


class PyreadstatTabularStub:
    """
    Configurable pyreadstat-style stub for SAV/XPT tabular handlers.
    """

    # pylint: disable=unused-argument

    def __init__(
        self,
        *,
        frame: DictRecordsFrameStub | None = None,
        read_method_name: str | None = None,
        write_method_name: str | None = None,
    ) -> None:
        self._frame = frame if frame is not None else DictRecordsFrameStub([])
        self._read_method_name = read_method_name
        self._write_method_name = write_method_name
        self.read_calls: list[str] = []
        self.write_calls: list[tuple[object, str]] = []

    def __getattr__(
        self,
        name: str,
    ) -> object:
        if name == self._read_method_name:
            return self._read_method
        if name == self._write_method_name:
            return self._write_method
        raise AttributeError(name)

    def _read_method(
        self,
        path: str,
    ) -> tuple[DictRecordsFrameStub, object]:
        self.read_calls.append(path)
        return self._frame, object()

    def _write_method(
        self,
        frame: object,
        path: str,
    ) -> None:
        self.write_calls.append((frame, path))

    def assert_single_read_path(
        self,
        path: Path,
    ) -> None:
        """Assert one pyreadstat read call using the provided path."""
        assert self.read_calls == [str(path)]

    def assert_last_write_path(
        self,
        path: Path,
    ) -> None:
        """Assert the most recent pyreadstat write call path."""
        assert self.write_calls
        _, write_path = self.write_calls[-1]
        assert write_path == str(path)


class RDataPandasStub:
    """
    Minimal pandas stub for R-data tests using record-frame conversion.
    """

    DataFrame = DictRecordsFrameStub


class ContextManagerSelfMixin:
    """
    Tiny mixin for stubs that act as context managers returning ``self``.
    """

    def __enter__(self) -> ContextManagerSelfMixin:
        return self

    def __exit__(
        self,
        exc_type: object,
        exc: object,
        tb: object,
    ) -> None:
        return None


class RDataNoWriterStub:
    """
    Minimal pyreadr-like stub exposing only ``read_r``.
    """

    # pylint: disable=unused-argument

    def read_r(
        self,
        path: str,
    ) -> dict[str, object]:
        """Return an empty mapping for reader-only flows."""
        return {}


class RecordsFrameStub:
    """Minimal frame stub that mimics pandas record/table APIs."""

    # pylint: disable=unused-argument

    def __init__(
        self,
        records: list[dict[str, object]],
    ) -> None:
        self._records = list(records)
        self.to_excel_calls: list[dict[str, object]] = []
        self.to_parquet_calls: list[dict[str, object]] = []
        self.to_feather_calls: list[dict[str, object]] = []
        self.to_orc_calls: list[dict[str, object]] = []

    def to_dict(
        self,
        *,
        orient: str,
    ) -> list[dict[str, object]]:  # noqa: ARG002
        """Return record payloads in ``records`` orientation."""
        return list(self._records)

    def to_excel(
        self,
        path: Path,
        *,
        index: bool,
        engine: str | None = None,
    ) -> None:
        """Record an Excel write call."""
        kwargs: dict[str, object] = {'index': index}
        if engine is not None:
            kwargs['engine'] = engine
        self._append_write_call(self.to_excel_calls, path, **kwargs)

    def to_feather(
        self,
        path: Path,
    ) -> None:
        """Record a feather write call."""
        self._append_write_call(self.to_feather_calls, path)

    def to_orc(
        self,
        path: Path,
        *,
        index: bool,
    ) -> None:
        """Record an ORC write call."""
        self._append_write_call(self.to_orc_calls, path, index=index)

    def to_parquet(
        self,
        path: Path,
        *,
        index: bool,
    ) -> None:
        """Record a parquet write call."""
        self._append_write_call(self.to_parquet_calls, path, index=index)

    @staticmethod
    def _append_write_call(
        calls: list[dict[str, object]],
        path: Path,
        **kwargs: object,
    ) -> None:
        """Append one writer-call record with path and keyword payload."""
        calls.append({'path': path, **kwargs})


class SpreadsheetSheetFrameStub(RecordsFrameStub):
    """Frame stub with optional support for ``sheet_name`` on writes."""

    # pylint: disable=unused-argument

    def __init__(
        self,
        records: list[dict[str, object]],
        *,
        allow_sheet_name: bool = True,
    ) -> None:
        super().__init__(records)
        self.allow_sheet_name = allow_sheet_name

    def to_excel(
        self,
        path: Path,
        **kwargs: object,
    ) -> None:
        """
        Record ``to_excel`` writes with optional ``sheet_name`` rejection.
        """
        self.to_excel_calls.append({'path': path, **kwargs})
        if not self.allow_sheet_name and 'sheet_name' in kwargs:
            raise TypeError('sheet_name not supported')


class SpreadsheetSheetPandasStub:
    """Pandas-like stub with configurable ``sheet_name`` support."""

    # pylint: disable=invalid-name, unused-argument

    def __init__(
        self,
        frame: SpreadsheetSheetFrameStub,
        *,
        read_supports_sheet_name: bool = True,
    ) -> None:
        self.frame = frame
        self.read_supports_sheet_name = read_supports_sheet_name
        self.read_calls: list[dict[str, object]] = []
        self.last_frame: SpreadsheetSheetFrameStub | None = None
        self.DataFrame = type(
            'DataFrame',
            (),
            {'from_records': staticmethod(self._from_records)},
        )

    def _from_records(
        self,
        records: list[dict[str, object]],
    ) -> SpreadsheetSheetFrameStub:
        created = SpreadsheetSheetFrameStub(
            records,
            allow_sheet_name=self.frame.allow_sheet_name,
        )
        self.last_frame = created
        return created

    def read_excel(
        self,
        path: Path,
        **kwargs: object,
    ) -> SpreadsheetSheetFrameStub:
        """Simulate ``pandas.read_excel`` with sheet-name support toggles."""
        self.read_calls.append({'path': path, **kwargs})
        if not self.read_supports_sheet_name and 'sheet_name' in kwargs:
            raise TypeError('sheet_name not supported')
        return self.frame
