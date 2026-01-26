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
from . import stub

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'read',
    'write',
]


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: Path,
) -> JSONList:
    """
    Read ODS content from *path*.

    Parameters
    ----------
    path : Path
        Path to the ODS file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the ODS file.
    """
    return stub.read(path, format_name='ODS')


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to ODS file at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the ODS file on disk.
    data : JSONData
        Data to write as ODS file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the ODS file.
    """
    return stub.write(path, data, format_name='ODS')
