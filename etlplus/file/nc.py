"""
:mod:`etlplus.file.nc` module.

Helpers for reading/writing NetCDF (NC) data files.

Notes
-----
- A NC file is a binary file format used for array-oriented scientific data,
    particularly in meteorology, oceanography, and climate science.
- Common cases:
    - Storing multi-dimensional scientific data.
    - Sharing large datasets in research communities.
    - Efficient data access and manipulation.
- Rule of thumb:
    - If the file follows the NetCDF standard, use this module for reading and
        writing.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from typing import cast

from ..types import JSONData
from ..types import JSONList
from ._imports import get_optional_module
from ._imports import get_pandas
from ._io import normalize_records

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'read',
    'write',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _get_xarray() -> Any:
    """Return the xarray module, importing it on first use."""
    return get_optional_module(
        'xarray',
        error_message=(
            'NC support requires optional dependency "xarray".\n'
            'Install with: pip install xarray'
        ),
    )


def _raise_engine_error(err: ImportError) -> None:
    raise ImportError(
        'NC support requires optional dependency "netCDF4" or "h5netcdf".\n'
        'Install with: pip install netCDF4',
    ) from err


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: Path,
) -> JSONList:
    """
    Read NC content from *path*.

    Parameters
    ----------
    path : Path
        Path to the NC file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the NC file.
    """
    xarray = _get_xarray()
    try:
        dataset = xarray.open_dataset(path)
    except ImportError as err:  # pragma: no cover
        _raise_engine_error(err)
    with dataset:
        frame = dataset.to_dataframe().reset_index()
    if 'index' in frame.columns:
        values = list(frame['index'])
        if values == list(range(len(values))):
            frame = frame.drop(columns=['index'])
    return cast(JSONList, frame.to_dict(orient='records'))


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to NC file at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the NC file on disk.
    data : JSONData
        Data to write as NC file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the NC file.
    """
    records = normalize_records(data, 'NC')
    if not records:
        return 0

    xarray = _get_xarray()
    pandas = get_pandas('NC')
    frame = pandas.DataFrame.from_records(records)
    dataset = xarray.Dataset.from_dataframe(frame)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        dataset.to_netcdf(path)
    except ImportError as err:  # pragma: no cover
        _raise_engine_error(err)
    return len(records)
