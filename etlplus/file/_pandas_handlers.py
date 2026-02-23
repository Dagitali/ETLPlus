"""
:mod:`etlplus.file._pandas_handlers` module.

Shared abstractions for pandas-backed file handlers.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from typing import ClassVar
from typing import NoReturn

from ..types import JSONData
from ..types import JSONList
from ._imports import get_pandas
from ._imports import get_pyarrow
from ._io import ensure_parent_dir
from ._io import normalize_records
from ._io import records_from_table
from .base import ColumnarFileHandlerABC
from .base import ReadOnlySpreadsheetFileHandlerABC
from .base import ReadOptions
from .base import SpreadsheetFileHandlerABC
from .base import WriteOptions

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Constants
    'PARQUET_DEPENDENCY_ERROR',
    # Classes
    'PandasColumnarHandlerMixin',
    'PandasReadOnlySpreadsheetHandlerMixin',
    'PandasSpreadsheetHandlerMixin',
]


# SECTION: CONSTANTS ======================================================== #


PARQUET_DEPENDENCY_ERROR = (
    'Parquet support requires optional dependency "pyarrow" or '
    '"fastparquet".\n'
    'Install with: pip install pyarrow'
)


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _raise_import_error(
    error: ImportError,
    *,
    message: str | None,
) -> NoReturn:
    """
    Raise one standardized import error while preserving cause.
    """
    if message is None:
        raise error
    raise ImportError(message) from error


def _read_excel_frame(
    pandas: Any,
    path: Path,
    *,
    sheet: str | int,
    engine: str | None = None,
) -> Any:
    """
    Read one spreadsheet frame, tolerating stubs without ``sheet_name``.
    """
    kwargs: dict[str, Any] = {'sheet_name': sheet}
    if engine is not None:
        kwargs['engine'] = engine
    try:
        return pandas.read_excel(path, **kwargs)
    except TypeError:
        kwargs.pop('sheet_name', None)
        return pandas.read_excel(path, **kwargs)


def _write_excel_frame(
    frame: Any,
    path: Path,
    *,
    sheet: str | int,
    engine: str | None = None,
) -> None:
    """
    Write one spreadsheet frame, tolerating stubs without ``sheet_name``.
    """
    kwargs: dict[str, Any] = {'index': False}
    if engine is not None:
        kwargs['engine'] = engine
    if isinstance(sheet, str):
        kwargs['sheet_name'] = sheet
    try:
        frame.to_excel(path, **kwargs)
    except TypeError:
        kwargs.pop('sheet_name', None)
        frame.to_excel(path, **kwargs)


# SECTION: CLASSES ========================================================== #


class PandasColumnarHandlerMixin(ColumnarFileHandlerABC):
    """
    Shared implementation for pandas-backed columnar handlers.
    """

    # -- Class Attributes -- #

    pandas_format_name: ClassVar[str]
    read_method: ClassVar[str]
    write_method: ClassVar[str]
    write_kwargs: ClassVar[tuple[tuple[str, Any], ...]] = ()
    requires_pyarrow: ClassVar[bool] = False
    import_error_message: ClassVar[str | None] = None

    # -- Instance Methods -- #

    def read_table(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> Any:
        """
        Read a columnar table object from *path*.
        """
        _ = options
        self.validate_runtime_dependencies()
        pandas = self.resolve_pandas()
        try:
            return getattr(pandas, self.read_method)(path)
        except ImportError as error:  # pragma: no cover
            _raise_import_error(
                error,
                message=self.import_error_message,
            )

    def records_to_table(
        self,
        data: JSONData,
    ) -> Any:
        """
        Convert row records into a pandas-backed table object.
        """
        self.validate_runtime_dependencies()
        pandas = self.resolve_pandas()
        records = normalize_records(data, self.format_name)
        return pandas.DataFrame.from_records(records)

    def resolve_pandas(self) -> Any:
        """
        Return the pandas module for this handler.
        """
        return get_pandas(self.pandas_format_name)

    def resolve_pyarrow(self) -> Any:
        """
        Return the pyarrow module for this handler.
        """
        return get_pyarrow(self.pandas_format_name)

    def table_to_records(
        self,
        table: Any,
    ) -> JSONList:
        """
        Convert a table object into row-oriented records.
        """
        return records_from_table(table)

    def validate_runtime_dependencies(self) -> None:
        """
        Validate optional dependencies required at runtime.
        """
        if self.requires_pyarrow:
            self.resolve_pyarrow()

    def write_table(
        self,
        path: Path,
        table: Any,
        *,
        options: WriteOptions | None = None,
    ) -> None:
        """
        Write a columnar table object to *path*.
        """
        _ = options
        self.validate_runtime_dependencies()
        kwargs = dict(self.write_kwargs)
        try:
            getattr(table, self.write_method)(path, **kwargs)
        except ImportError as error:  # pragma: no cover
            _raise_import_error(
                error,
                message=self.import_error_message,
            )


class PandasSpreadsheetHandlerMixin(SpreadsheetFileHandlerABC):
    """
    Shared implementation for writable pandas-backed spreadsheet handlers.
    """

    # -- Class Attributes -- #

    pandas_format_name: ClassVar[str]
    read_engine: ClassVar[str | None] = None
    write_engine: ClassVar[str | None] = None
    import_error_message: ClassVar[str | None] = None

    # -- Instance Methods -- #

    def read_sheet(
        self,
        path: Path,
        *,
        sheet: str | int,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read one sheet from *path*.
        """
        _ = options
        pandas = self.resolve_pandas()
        try:
            frame = _read_excel_frame(
                pandas,
                path,
                sheet=sheet,
                engine=self.read_engine,
            )
        except ImportError as error:  # pragma: no cover
            _raise_import_error(
                error,
                message=self.import_error_message,
            )
        return records_from_table(frame)

    def resolve_pandas(self) -> Any:
        """
        Return the pandas module for this handler.
        """
        return get_pandas(self.pandas_format_name)

    def write_sheet(
        self,
        path: Path,
        rows: JSONList,
        *,
        sheet: str | int,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write one sheet to *path*.
        """
        _ = options
        ensure_parent_dir(path)
        pandas = self.resolve_pandas()
        frame = pandas.DataFrame.from_records(rows)
        try:
            _write_excel_frame(
                frame,
                path,
                sheet=sheet,
                engine=self.write_engine,
            )
        except ImportError as error:  # pragma: no cover
            _raise_import_error(
                error,
                message=self.import_error_message,
            )
        return len(rows)


class PandasReadOnlySpreadsheetHandlerMixin(
    ReadOnlySpreadsheetFileHandlerABC,
):
    """
    Shared implementation for read-only pandas-backed spreadsheet handlers.
    """

    # -- Class Attributes -- #

    pandas_format_name: ClassVar[str]
    read_engine: ClassVar[str | None] = None
    import_error_message: ClassVar[str | None] = None

    # -- Instance Methods -- #

    def read_sheet(
        self,
        path: Path,
        *,
        sheet: str | int,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read one sheet from *path*.
        """
        _ = options
        pandas = self.resolve_pandas()
        try:
            frame = _read_excel_frame(
                pandas,
                path,
                sheet=sheet,
                engine=self.read_engine,
            )
        except ImportError as error:  # pragma: no cover
            _raise_import_error(
                error,
                message=self.import_error_message,
            )
        return records_from_table(frame)

    def resolve_pandas(self) -> Any:
        """
        Return the pandas module for this handler.
        """
        return get_pandas(self.pandas_format_name)
