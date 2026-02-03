"""
:mod:`etlplus.file.sas7bdat` module.

Helpers for reading/writing SAS (SAS7BDAT) data files.

Notes
-----
- A SAS7BDAT file is a proprietary binary file format created by SAS to store
    datasets, including variables, labels, and data types.
- Common cases:
    - Statistical analysis pipelines.
    - Data exchange with SAS tooling.
- Rule of thumb:
    - If the file follows the SAS7BDAT specification, use this module for
        reading and writing.
"""

from __future__ import annotations

from typing import cast

from ..types import JSONData
from ..types import JSONList
from ..types import StrPath
from . import stub
from ._imports import get_dependency
from ._imports import get_pandas
from ._io import coerce_path

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
    Read SAS7BDAT content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the SAS7BDAT file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the SAS7BDAT file.
    """
    path = coerce_path(path)
    get_dependency('pyreadstat', format_name='SAS7BDAT')
    pandas = get_pandas('SAS7BDAT')
    try:
        frame = pandas.read_sas(path, format='sas7bdat')
    except TypeError:
        frame = pandas.read_sas(path)
    return cast(JSONList, frame.to_dict(orient='records'))


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Write *data* to SAS7BDAT file at *path* and return record count.

    Parameters
    ----------
    path : StrPath
        Path to the SAS7BDAT file on disk.
    data : JSONData
        Data to write as SAS7BDAT file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the SAS7BDAT file.
    """
    path = coerce_path(path)
    return stub.write(path, data, format_name='SAS7BDAT')
