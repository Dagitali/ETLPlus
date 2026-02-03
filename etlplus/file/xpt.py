"""
:mod:`etlplus.file.xpt` module.

Helpers for reading/writing SAS Transport (XPT) files.

Notes
-----
- A SAS Transport (XPT) file is a standardized file format used to transfer
    SAS datasets between different systems.
- Common cases:
    - Sharing datasets between different SAS installations.
    - Archiving datasets in a platform-independent format.
    - Importing/exporting data to/from statistical software that supports XPT.
- Rule of thumb:
    - If you need to work with XPT files, use this module for reading
        and writing.
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


# SECTION: INTERNAL HELPERS ================================================ #


def _get_pyreadstat() -> Any:
    """Return the pyreadstat module, importing it on first use."""
    return get_optional_module(
        'pyreadstat',
        error_message=(
            'XPT support requires optional dependency "pyreadstat".\n'
            'Install with: pip install pyreadstat'
        ),
    )


def _raise_readstat_error(err: ImportError) -> None:
    raise ImportError(
        'XPT support requires optional dependency "pyreadstat".\n'
        'Install with: pip install pyreadstat',
    ) from err


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: Path,
) -> JSONList:
    """
    Read XPT content from *path*.

    Parameters
    ----------
    path : Path
        Path to the XPT file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the XPT file.
    """
    pandas = get_pandas('XPT')
    pyreadstat = _get_pyreadstat()
    reader = getattr(pyreadstat, 'read_xport', None)
    if reader is not None:
        frame, _meta = reader(str(path))
        return cast(JSONList, frame.to_dict(orient='records'))
    try:
        frame = pandas.read_sas(path, format='xport')
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
    Write *data* to XPT file at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the XPT file on disk.
    data : JSONData
        Data to write as XPT file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the XPT file.

    Raises
    ------
    ImportError
        If "pyreadstat" is not installed with write support.
    """
    records = normalize_records(data, 'XPT')
    if not records:
        return 0

    pandas = get_pandas('XPT')
    pyreadstat = _get_pyreadstat()
    writer = getattr(pyreadstat, 'write_xport', None)
    if writer is None:
        raise ImportError(
            'XPT write support requires "pyreadstat" with write_xport().',
        )

    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pandas.DataFrame.from_records(records)
    writer(frame, str(path))
    return len(records)
