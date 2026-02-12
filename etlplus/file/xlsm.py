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

from ..types import JSONData
from ..types import JSONList
from ..types import StrPath
from ._imports import get_pandas
from ._io import call_deprecated_module_read
from ._io import call_deprecated_module_write
from ._io import ensure_parent_dir
from ._io import records_from_table
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
        return records_from_table(frame)

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


# SECTION: INTERNAL CONSTANTS =============================================== #

_XLSM_HANDLER = XlsmFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Deprecated wrapper. Use ``XlsmFile().read(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the XLSM file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the XLSM file.
    """
    return call_deprecated_module_read(
        path,
        __name__,
        _XLSM_HANDLER.read,
    )


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Deprecated wrapper. Use ``XlsmFile().write(...)`` instead.

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
    return call_deprecated_module_write(
        path,
        data,
        __name__,
        _XLSM_HANDLER.write,
    )
