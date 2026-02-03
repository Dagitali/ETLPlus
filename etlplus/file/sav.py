"""
:mod:`etlplus.file.sav` module.

Helpers for reading/writing SPSS (SAV) files.

Notes
-----
- A SAV file is a dataset created by SPSS.
- Common cases:
    - Survey and market research datasets.
    - Statistical analysis workflows.
    - Exchange with SPSS and compatible tools.
- Rule of thumb:
    - If the file follows the SAV specification, use this module for reading
        and writing.
"""

from __future__ import annotations

from typing import cast

from ..types import JSONData
from ..types import JSONList
from ..types import StrPath
from ._imports import get_dependency
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
    Read SAV content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the SAV file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the SAV file.
    """
    path = coerce_path(path)
    pyreadstat = get_dependency('pyreadstat', format_name='SAV')
    frame, _meta = pyreadstat.read_sav(str(path))
    return cast(JSONList, frame.to_dict(orient='records'))


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Write *data* to SAV at *path* and return record count.

    Parameters
    ----------
    path : StrPath
        Path to the SAV file on disk.
    data : JSONData
        Data to write as SAV. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the SAV file.
    """
    path = coerce_path(path)
    records = normalize_records(data, 'SAV')
    if not records:
        return 0

    pyreadstat = get_dependency('pyreadstat', format_name='SAV')
    pandas = get_pandas('SAV')
    ensure_parent_dir(path)
    frame = pandas.DataFrame.from_records(records)
    pyreadstat.write_sav(frame, str(path))
    return len(records)
