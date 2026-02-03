"""
:mod:`etlplus.file.tab` module.

Helpers for reading/writing "tab"-formatted (TAB) files.

Notes
-----
- A TAB file is not necessarily a TSV file when tabs aren’t actually the
    delimiter that defines the fields, even if the text looks column-aligned.
- Common cases:
    - Fixed-width text (FWF) that uses tabs for alignment.
    - Mixed whitespace (tabs + spaces) as “pretty printing”.
    - Tabs embedded inside quoted fields (or unescaped tabs in free text).
    - Header/metadata lines or multi-line records that break TSV assumptions.
    - Not actually tab-delimited despite the name.
- Rule of thumb:
    - This implementation treats TAB as tab-delimited text.
    - If the file has fixed-width fields, use :mod:`etlplus.file.fwf`.
"""

from __future__ import annotations

from ..types import JSONData
from ..types import JSONList
from ..types import StrPath
from ._io import coerce_path
from ._io import read_delimited
from ._io import write_delimited

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'read',
    'write',
]


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Read TAB content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the TAB file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the TAB file.
    """
    return read_delimited(path, delimiter='\t')


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Write *data* to TAB file at *path* and return record count.

    Parameters
    ----------
    path : StrPath
        Path to the TAB file on disk.
    data : JSONData
        Data to write as TAB file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the TAB file.
    """
    path = coerce_path(path)
    return write_delimited(path, data, delimiter='\t', format_name='TAB')
