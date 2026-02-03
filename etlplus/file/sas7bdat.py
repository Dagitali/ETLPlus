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

from pathlib import Path
from typing import Any
from typing import cast

from ..types import JSONData
from ..types import JSONList
from . import stub
from ._imports import get_optional_module
from ._imports import get_pandas

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'read',
    'write',
]


# SECTION: INTERNAL HELPERS ================================================ #


def _get_pyreadstat() -> Any:
    """Return the pyreadstat module, importing it on first use."""
    return get_optional_module(
        'pyreadstat',
        error_message=(
            'SAS7BDAT support requires optional dependency "pyreadstat".\n'
            'Install with: pip install pyreadstat'
        ),
    )


def _raise_readstat_error(err: ImportError) -> None:
    raise ImportError(
        'SAS7BDAT support requires optional dependency "pyreadstat".\n'
        'Install with: pip install pyreadstat',
    ) from err


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: Path,
) -> JSONList:
    """
    Read SAS7BDAT content from *path*.

    Parameters
    ----------
    path : Path
        Path to the SAS7BDAT file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the SAS7BDAT file.
    """
    pandas = get_pandas('SAS7BDAT')
    try:
        frame = pandas.read_sas(path, format='sas7bdat')
    except TypeError:
        frame = pandas.read_sas(path)
    except ImportError as err:  # pragma: no cover
        _raise_readstat_error(err)
    return cast(JSONList, frame.to_dict(orient='records'))


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to SAS7BDAT file at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the SAS7BDAT file on disk.
    data : JSONData
        Data to write as SAS7BDAT file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the SAS7BDAT file.
    """
    return stub.write(path, data, format_name='SAS7BDAT')
