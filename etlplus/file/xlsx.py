"""
:mod:`etlplus.file.xlsx` module.

Helpers for reading/writing Excel XLSX files.
"""

from __future__ import annotations

from pathlib import Path
from typing import cast

from ..types import JSONData
from ..types import JSONList
from ..types import StrPath
from ._imports import get_pandas
from ._io import coerce_path
from ._io import ensure_parent_dir
from ._io import normalize_records
from ._io import warn_deprecated_module_io
from .base import ReadOptions
from .base import SpreadsheetFileHandlerABC
from .base import WriteOptions
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'XlsxFile',
    # Functions
    'read',
    'write',
]

# SECTION: CLASSES ========================================================== #


class XlsxFile(SpreadsheetFileHandlerABC):
    """
    Handler implementation for XLSX files.
    """

    # -- Class Attributes -- #

    format = FileFormat.XLSX
    engine_name = 'openpyxl'

    # -- Instance Methods -- #

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read and return XLSX content from *path*.

        Parameters
        ----------
        path : Path
            Path to the XLSX file on disk.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONList
            The list of dictionaries read from the XLSX file.
        """
        sheet = self.sheet_from_read_options(options)
        return self.read_sheet(path, sheet=sheet, options=options)

    def read_sheet(
        self,
        path: Path,
        *,
        sheet: str | int,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read one XLSX sheet from *path*.

        Parameters
        ----------
        path : Path
            Path to the XLSX file on disk.
        sheet : str | int
            Sheet selector (name or index).
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONList
            The list of dictionaries read from the XLSX sheet.

        Raises
        ------
        ImportError
            If the required optional dependency is not installed.
        """
        _ = options
        pandas = get_pandas('XLSX')
        try:
            frame = pandas.read_excel(path, sheet_name=sheet)
        except TypeError:
            # Test stubs and older adapters may not accept sheet_name.
            frame = pandas.read_excel(path)
        except ImportError as err:  # pragma: no cover
            raise ImportError(
                'XLSX support requires optional dependency "openpyxl".\n'
                'Install with: pip install openpyxl',
            ) from err
        return cast(JSONList, frame.to_dict(orient='records'))

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write *data* to XLSX at *path* and return record count.

        Parameters
        ----------
        path : Path
            Path to the XLSX file on disk.
        data : JSONData
            Data to write.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        int
            Number of records written.
        """
        records = normalize_records(data, 'XLSX')
        if not records:
            return 0
        sheet = self.sheet_from_write_options(options)
        return self.write_sheet(path, records, sheet=sheet, options=options)

    def write_sheet(
        self,
        path: Path,
        rows: JSONList,
        *,
        sheet: str | int,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write rows to one XLSX sheet in *path*.

        Parameters
        ----------
        path : Path
            Path to the XLSX file on disk.
        rows : JSONList
            Rows to write.
        sheet : str | int
            Sheet selector (name or index).
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        int
            Number of rows written.

        Raises
        ------
        ImportError
            If the required optional dependency is not installed.
        """
        _ = options
        pandas = get_pandas('XLSX')
        ensure_parent_dir(path)
        frame = pandas.DataFrame.from_records(rows)
        try:
            if isinstance(sheet, str):
                frame.to_excel(path, index=False, sheet_name=sheet)
            else:
                frame.to_excel(path, index=False)
        except TypeError:
            frame.to_excel(path, index=False)
        except ImportError as err:  # pragma: no cover
            raise ImportError(
                'XLSX support requires optional dependency "openpyxl".\n'
                'Install with: pip install openpyxl',
            ) from err
        return len(rows)


# SECTION: INTERNAL CONSTANTS =============================================== #

_XLSX_HANDLER = XlsxFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Read and return XLSX content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the XLSX file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the XLSX file.
    """
    warn_deprecated_module_io(__name__, 'read')
    return _XLSX_HANDLER.read(coerce_path(path))


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Write *data* to XLSX at *path* and return record count.

    Parameters
    ----------
    path : StrPath
        Path to the XLSX file on disk.
    data : JSONData
        Data to write.

    Returns
    -------
    int
        Number of records written.
    """
    warn_deprecated_module_io(__name__, 'write')
    return _XLSX_HANDLER.write(coerce_path(path), data)
