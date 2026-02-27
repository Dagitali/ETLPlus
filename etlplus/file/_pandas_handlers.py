"""
:mod:`etlplus.file._pandas_handlers` module.

Shared abstractions for pandas-backed file handlers.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from typing import ClassVar

from ..utils.types import JSONData
from ..utils.types import JSONList
from ._dataframe import dataframe_from_data
from ._dataframe import dataframe_from_records
from ._imports import resolve_dependency
from ._imports import resolve_module_callable
from ._imports import resolve_pandas as resolve_pandas_dependency
from ._io import ensure_parent_dir
from ._io import records_from_table
from .base import ColumnarFileHandlerABC
from .base import ReadOnlySpreadsheetFileHandlerABC
from .base import ReadOptions
from .base import SpreadsheetFileHandlerABC
from .base import WriteOptions

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'PandasColumnarHandlerMixin',
    'PandasReadOnlySpreadsheetHandlerMixin',
    'PandasSpreadsheetHandlerMixin',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


_SPREADSHEET_ENGINE_DEPENDENCIES: dict[str, tuple[str, str | None]] = {
    'odf': ('odf', 'odfpy'),
    'openpyxl': ('openpyxl', None),
    'xlrd': ('xlrd', None),
}


def _read_excel_frame(
    pandas: Any,
    path: Path,
    *,
    sheet: str | int,
    engine: str | None = None,
) -> Any:
    """
    Read one spreadsheet frame, tolerating stubs without ``sheet_name``.

    Parameters
    ----------
    pandas : Any
        The pandas module to use for reading.
    path : Path
        Path to the spreadsheet file on disk.
    sheet : str | int
        Sheet selector, by name or index.
    engine : str | None
        Optional read engine name.

    Returns
    -------
    Any
        The read DataFrame.
    """
    kwargs: dict[str, Any] = {'sheet_name': sheet}
    if engine is not None:
        kwargs['engine'] = engine
    try:
        return pandas.read_excel(path, **kwargs)
    except TypeError:
        kwargs.pop('sheet_name', None)
        return pandas.read_excel(path, **kwargs)


def _read_sheet_records(
    *,
    path: Path,
    sheet: str | int,
    pandas: Any,
    engine: str | None,
) -> JSONList:
    """
    Read one spreadsheet sheet and return row records.

    Parameters
    ----------
    path : Path
        Path to the spreadsheet file on disk.
    sheet : str | int
        Sheet selector, by name or index.
    pandas : Any
        The pandas module to use for reading.
    engine : str | None
        Optional read engine name.

    Returns
    -------
    JSONList
        Row records parsed from the selected sheet.
    """
    frame = _read_excel_frame(
        pandas,
        path,
        sheet=sheet,
        engine=engine,
    )
    return records_from_table(frame)


def _resolve_pyarrow_dependency(
    handler: object,
    *,
    format_name: str,
) -> Any:
    """
    Resolve required pyarrow, preferring concrete-module resolver when present.

    Parameters
    ----------
    handler : object
        The handler instance for which to resolve the dependency.
    format_name : str
        Human-readable format name for error messages.

    Returns
    -------
    Any
        The pyarrow module.
    """
    if resolver := resolve_module_callable(handler, 'get_pyarrow'):
        return resolver(format_name)
    return resolve_dependency(
        handler,
        'pyarrow',
        format_name=format_name,
        required=True,
    )


def _resolve_spreadsheet_engine_dependency(
    handler: object,
    *,
    engine: str | None,
    format_name: str,
) -> None:
    """
    Resolve required spreadsheet-engine dependency for one handler operation.

    Parameters
    ----------
    handler : object
        The handler instance for which to resolve the dependency.
    engine : str | None
        Spreadsheet engine name, if known.
    format_name : str
        Human-readable format name for import error messages.
    """
    if engine is None:
        return
    if (spec := _SPREADSHEET_ENGINE_DEPENDENCIES.get(engine)) is None:
        return
    module_name, pip_name = spec
    resolve_dependency(
        handler,
        module_name,
        format_name=format_name,
        pip_name=pip_name,
        required=True,
    )


def _write_excel_frame(
    frame: Any,
    path: Path,
    *,
    sheet: str | int,
    engine: str | None = None,
) -> None:
    """
    Write one spreadsheet frame, tolerating stubs without ``sheet_name``.

    Parameters
    ----------
    frame : Any
        The spreadsheet frame to write.
    path : Path
        Path to the spreadsheet file on disk.
    sheet : str | int
        Sheet selector, by name or index.
    engine : str | None, optional
        Optional write engine name.
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


class _PandasModuleResolverMixin:
    """
    Resolve pandas via shared compatibility-first dependency lookup.
    """

    pandas_format_name: ClassVar[str]

    def resolve_pandas(self) -> Any:
        """
        Return the pandas module for this handler.
        """
        return resolve_pandas_dependency(
            self,
            format_name=self.pandas_format_name,
        )


class _PandasSpreadsheetReadMixin(_PandasModuleResolverMixin):
    """
    Shared read path for pandas-backed spreadsheet handlers.
    """

    engine_name: ClassVar[str]
    read_engine: ClassVar[str | None] = None

    def read_sheet(
        self,
        path: Path,
        *,
        sheet: str | int,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read one sheet from *path*.

        Parameters
        ----------
        path : Path
            Path to the spreadsheet file on disk.
        sheet : str | int
            Sheet selector, by name or index.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONList
            Parsed records from the selected sheet.
        """
        _ = options
        engine = self.read_engine or self.engine_name
        _resolve_spreadsheet_engine_dependency(
            self,
            engine=engine,
            format_name=self.pandas_format_name,
        )
        return _read_sheet_records(
            path=path,
            sheet=sheet,
            pandas=self.resolve_pandas(),
            engine=engine,
        )


class PandasColumnarHandlerMixin(
    _PandasModuleResolverMixin,
    ColumnarFileHandlerABC,
):
    """
    Shared implementation for pandas-backed columnar handlers.
    """

    # -- Class Attributes -- #

    pandas_format_name: ClassVar[str]
    read_method: ClassVar[str]
    write_method: ClassVar[str]
    write_kwargs: ClassVar[tuple[tuple[str, Any], ...]] = ()
    requires_pyarrow: ClassVar[bool] = False

    # -- Instance Methods -- #

    def read_table(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> Any:
        """
        Read a columnar table object from *path*.

        Parameters
        ----------
        path : Path
            Path to the file on disk.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        Any
            Columnar table object read from the file.
        """
        _ = options
        self.validate_runtime_dependencies()
        pandas = self.resolve_pandas()
        return getattr(pandas, self.read_method)(path)

    def records_to_table(
        self,
        data: JSONData,
    ) -> Any:
        """
        Convert row records into a pandas-backed table object.
        """
        self.validate_runtime_dependencies()
        return dataframe_from_data(
            self.resolve_pandas(),
            data,
            format_name=self.format_name,
        )

    def resolve_pyarrow(self) -> Any:
        """
        Return the pyarrow module for this handler.
        """
        return _resolve_pyarrow_dependency(
            self,
            format_name=self.pandas_format_name,
        )

    def table_to_records(
        self,
        table: Any,
    ) -> JSONList:
        """
        Convert a table object into row-oriented records.

        Parameters
        ----------
        table : Any
            Columnar table object to convert.

        Returns
        -------
        JSONList
            Row-oriented records extracted from the table.
        """
        return records_from_table(table)

    def validate_runtime_dependencies(self) -> None:
        """
        Validate runtime dependencies required by this handler.
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

        Parameters
        ----------
        path : Path
            Path to the file on disk.
        table : Any
            Columnar table object to write.
        options : WriteOptions | None, optional
            Optional write parameters.
        """
        _ = options
        self.validate_runtime_dependencies()
        kwargs = dict(self.write_kwargs)
        getattr(table, self.write_method)(path, **kwargs)


class PandasSpreadsheetHandlerMixin(
    _PandasSpreadsheetReadMixin,
    SpreadsheetFileHandlerABC,
):
    """
    Shared implementation for writable pandas-backed spreadsheet handlers.
    """

    # -- Class Attributes -- #

    pandas_format_name: ClassVar[str]
    write_engine: ClassVar[str | None] = None

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

        Parameters
        ----------
        path : Path
            Path to the spreadsheet file on disk.
        rows : JSONList
            Row records to write to the sheet.
        sheet : str | int
            Sheet selector, by name or index.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        int
            The number of records written to the sheet.
        """
        _ = options
        engine = self.write_engine or self.engine_name
        _resolve_spreadsheet_engine_dependency(
            self,
            engine=engine,
            format_name=self.pandas_format_name,
        )
        ensure_parent_dir(path)
        pandas = self.resolve_pandas()
        frame = dataframe_from_records(pandas, rows)
        _write_excel_frame(
            frame,
            path,
            sheet=sheet,
            engine=engine,
        )
        return len(rows)


class PandasReadOnlySpreadsheetHandlerMixin(
    _PandasSpreadsheetReadMixin,
    ReadOnlySpreadsheetFileHandlerABC,
):
    """
    Shared implementation for read-only pandas-backed spreadsheet handlers.
    """

    # -- Class Attributes -- #

    pandas_format_name: ClassVar[str]
