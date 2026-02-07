"""
:mod:`etlplus.file.xlsm` module.

Helpers for reading/writing Microsoft Excel Macro-Enabled (XLSM)
spreadsheet files.

Notes
-----
- An XLSM file is a spreadsheet file created using the Microsoft Excel Macro-
    Enabled (Open XML) format.
- Common cases:
    - Reading data from Excel Macro-Enabled spreadsheets.
    - Writing data to Excel Macro-Enabled format for compatibility.
    - Converting XLSM files to more modern formats.
- Rule of thumb:
    - If you need to work with Excel Macro-Enabled spreadsheet files, use this
        module for reading and writing.
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
from .base import ReadOptions
from .base import SpreadsheetFileHandlerABC
from .base import WriteOptions
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'XlsmFile',
    # Functions
    'read',
    'write',
]

# SECTION: CLASSES ========================================================== #


class XlsmFile(SpreadsheetFileHandlerABC):
    """
    Handler implementation for XLSM files.
    """

    # -- Class Attributes -- #

    format = FileFormat.XLSM
    engine_name = 'openpyxl'

    # -- Instance Methods -- #

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read and return XLSM content from *path*.

        Parameters
        ----------
        path : Path
            Path to the XLSM file on disk.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONList
            The list of dictionaries read from the XLSM file.
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
        Read one XLSM sheet from *path*.

        Parameters
        ----------
        path : Path
            Path to the XLSM file on disk.
        sheet : str | int
            Sheet selector (name or index).
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONList
            The list of dictionaries read from the XLSM sheet.

        Raises
        ------
        ImportError
            If the required optional dependency is not installed.
        """
        _ = options
        pandas = get_pandas('XLSM')
        try:
            frame = pandas.read_excel(path, sheet_name=sheet)
        except TypeError:
            frame = pandas.read_excel(path)
        except ImportError as err:  # pragma: no cover
            raise ImportError(
                'XLSM support requires optional dependency "openpyxl".\n'
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
        Write *data* to XLSM at *path* and return record count.

        Parameters
        ----------
        path : Path
            Path to the XLSM file on disk.
        data : JSONData
            Data to write as XLSM file.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        int
            The number of rows written to the XLSM file.
        """
        records = normalize_records(data, 'XLSM')
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
        Write rows to one XLSM sheet in *path*.

        Parameters
        ----------
        path : Path
            Path to the XLSM file on disk.
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
        pandas = get_pandas('XLSM')
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
                'XLSM support requires optional dependency "openpyxl".\n'
                'Install with: pip install openpyxl',
            ) from err
        return len(rows)


# SECTION: INTERNAL CONSTANTS ============================================== #


_XLSM_HANDLER = XlsmFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Read and return XLSM content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the XLSM file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the XLSM file.
    """
    return _XLSM_HANDLER.read(coerce_path(path))


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Write *data* to XLSM at *path* and return record count.

    Parameters
    ----------
    path : StrPath
        Path to the XLSM file on disk.
    data : JSONData
        Data to write as XLSM file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the XLSM file.
    """
    return _XLSM_HANDLER.write(coerce_path(path), data)
