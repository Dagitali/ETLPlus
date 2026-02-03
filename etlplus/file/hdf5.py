"""
:mod:`etlplus.file.hdf5` module.

Helpers for reading Hierarchical Data Format (HDF5) files. Stub helpers for
writing such files (not implemented yet).

Notes
-----
- A HDF5 file is a binary file format designed to store and organize large
    amounts of data.
- Common cases:
    - Scientific data storage and sharing.
    - Large-scale data analysis.
    - Hierarchical data organization.
- Rule of thumb:
    - If the file follows the HDF5 specification, use this module for reading
        and writing.
"""

from __future__ import annotations

from typing import cast

from ..types import JSONData
from ..types import JSONList
from ..types import StrPath
from . import stub
from ._imports import get_pandas
from ._io import coerce_path

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'read',
    'write',
]


# SECTION: INTERNAL CONSTANTS ============================================== #


DEFAULT_KEY = 'data'


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _raise_tables_error(
    err: ImportError,
) -> None:
    """
    Raise a consistent ImportError for missing PyTables support.

    Parameters
    ----------
    err : ImportError
        The original ImportError raised when trying to use HDF5 support without
        the required dependency.

    Raises
    ------
    ImportError
        Consistent ImportError indicating that PyTables is required.
    """
    raise ImportError(
        'HDF5 support requires optional dependency "tables".\n'
        'Install with: pip install tables',
    ) from err


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Read HDF5 content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the HDF5 file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the HDF5 file.

    Raises
    ------
    ValueError
        If multiple datasets are found in the HDF5 file without a clear key to
        use.
    """
    path = coerce_path(path)
    pandas = get_pandas('HDF5')
    try:
        store = pandas.HDFStore(path)
    except ImportError as err:  # pragma: no cover
        _raise_tables_error(err)

    with store:
        keys = [key.lstrip('/') for key in store.keys()]
        if not keys:
            return []
        if DEFAULT_KEY in keys:
            key = DEFAULT_KEY
        elif len(keys) == 1:
            key = keys[0]
        else:
            raise ValueError(
                'Multiple datasets found in HDF5 file; expected "data" or '
                'a single dataset',
            )
        frame = store.get(key)
    return cast(JSONList, frame.to_dict(orient='records'))


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Write *data* to HDF5 file at *path* and return record count.

    Parameters
    ----------
    path : StrPath
        Path to the HDF5 file on disk.
    data : JSONData
        Data to write as HDF5 file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the HDF5 file.
    """
    path = coerce_path(path)
    return stub.write(path, data, format_name='HDF5')
