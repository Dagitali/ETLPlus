"""
:mod:`etlplus.file.rda` module.

Helpers for reading/writing RData workspace/object bundle (RDA) files.

Notes
-----
- A RDA file is a binary file format used by R to store workspace objects,
    including data frames, lists, and other R objects.
- Common cases:
    - Storing R data objects for later use.
    - Sharing R datasets between users.
    - Loading R data into Python for analysis.
- Rule of thumb:
    - If the file follows the RDA specification, use this module for reading
        and writing.
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
    Read RDA content from *path*.

    Parameters
    ----------
    path : Path
        Path to the RDA file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the RDA file.
    """
    return stub.read(path, format_name='RDA')


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to RDA file at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the RDA file on disk.
    data : JSONData
        Data to write as RDA file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the RDA file.
    """
    return stub.write(path, data, format_name='RDA')
