"""
:mod:`etlplus.file.ods` module.

Helpers for reading/writing OpenDocument (ODS) spreadsheet files.

Notes
-----
- An ODS file is a spreadsheet file created using the OpenDocument format.
- Common cases:
    - Spreadsheet files created by LibreOffice Calc, Apache OpenOffice Calc, or
        other applications that support the OpenDocument format.
    - Spreadsheet files exchanged in open standards environments.
    - Spreadsheet files used in government or educational institutions
        promoting open formats.
- Rule of thumb:
    - If the file follows the OpenDocument specification, use this module for
        reading and writing.
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
    'OdsFile',
    # Functions
    'read',
    'write',
]

# SECTION: CLASSES ========================================================== #


class OdsFile(SpreadsheetFileHandlerABC):
    """
    Handler implementation for ODS files.
    """

    # -- Class Attributes -- #

    format = FileFormat.ODS
    engine_name = 'odf'

    # -- Instance Methods -- #

    def read_sheet(
        self,
        path: Path,
        *,
        sheet: str | int,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read one ODS sheet from *path*.

        Parameters
        ----------
        path : Path
            Path to the ODS file on disk.
        sheet : str | int
            Sheet selector (name or index).
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONList
            The list of dictionaries read from the ODS sheet.

        Raises
        ------
        ImportError
            If the required optional dependency for ODS support is not
            installed.
        """
        _ = options
        pandas = get_pandas('ODS')
        try:
            frame = pandas.read_excel(
                path,
                engine='odf',
                sheet_name=sheet,
            )
        except TypeError:
            frame = pandas.read_excel(path, engine='odf')
        except ImportError as err:  # pragma: no cover
            raise ImportError(
                'ODS support requires optional dependency "odfpy".\n'
                'Install with: pip install odfpy',
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
        Write rows to one ODS sheet in *path*.

        Parameters
        ----------
        path : Path
            Path to the ODS file on disk.
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
            If the required optional dependency for ODS support is not
            installed.
        """
        _ = options
        pandas = get_pandas('ODS')
        ensure_parent_dir(path)
        frame = pandas.DataFrame.from_records(rows)
        try:
            if isinstance(sheet, str):
                frame.to_excel(
                    path,
                    index=False,
                    engine='odf',
                    sheet_name=sheet,
                )
            else:
                frame.to_excel(path, index=False, engine='odf')
        except TypeError:
            frame.to_excel(path, index=False, engine='odf')
        except ImportError as err:  # pragma: no cover
            raise ImportError(
                'ODS support requires optional dependency "odfpy".\n'
                'Install with: pip install odfpy',
            ) from err
        return len(rows)


# SECTION: INTERNAL CONSTANTS =============================================== #

_ODS_HANDLER = OdsFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Deprecated wrapper. Use ``OdsFile().read(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the ODS file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the ODS file.
    """
    return call_deprecated_module_read(
        path,
        __name__,
        _ODS_HANDLER.read,
    )


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Deprecated wrapper. Use ``OdsFile().write(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the ODS file on disk.
    data : JSONData
        Data to write as ODS. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the ODS file.
    """
    return call_deprecated_module_write(
        path,
        data,
        __name__,
        _ODS_HANDLER.write,
    )
