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

from typing import cast

from ..types import JSONData
from ..types import JSONList
from ..types import StrPath
from ._imports import get_pandas
from ._io import coerce_path
from ._io import ensure_parent_dir
from ._io import normalize_records

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
    Read ODS content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the ODS file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the ODS file.

    Raises
    ------
    ImportError
        If optional dependencies for ODS support are missing.
    """
    path = coerce_path(path)
    pandas = get_pandas('ODS')
    try:
        frame = pandas.read_excel(path, engine='odf')
    except ImportError as err:  # pragma: no cover
        raise ImportError(
            'ODS support requires optional dependency "odfpy".\n'
            'Install with: pip install odfpy',
        ) from err
    return cast(JSONList, frame.to_dict(orient='records'))


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Write *data* to ODS file at *path* and return record count.

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

    Raises
    ------
    ImportError
        If optional dependencies for ODS support are missing.
    """
    path = coerce_path(path)
    records = normalize_records(data, 'ODS')
    if not records:
        return 0

    pandas = get_pandas('ODS')
    ensure_parent_dir(path)
    frame = pandas.DataFrame.from_records(records)
    try:
        frame.to_excel(path, index=False, engine='odf')
    except ImportError as err:  # pragma: no cover
        raise ImportError(
            'ODS support requires optional dependency "odfpy".\n'
            'Install with: pip install odfpy',
        ) from err
    return len(records)
