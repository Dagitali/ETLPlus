"""
:mod:`etlplus.file.stub` module.

Helpers for reading/writing stubbed files.
"""

from __future__ import annotations

from pathlib import Path

from ..types import JSONData
from ..types import JSONList

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'read',
    'write',
]


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: Path,
    format_name: str = 'Stubbed',
) -> JSONList:
    """
    Raises a :class:`NotImplementedError` for stubbed reads.

    Parameters
    ----------
    path : Path
        Path to the stubbed file on disk.
    format_name : str
        Human-readable format name.

    Returns
    -------
    JSONList
        The list of dictionaries read from the stubbed file.

    Raises
    ------
    NotImplementedError
        Always, since this is a stub implementation.
    """
    _ = path
    raise NotImplementedError(f'{format_name} read is not implemented yet')


def write(
    path: Path,
    data: JSONData,
    format_name: str = 'Stubbed',
) -> int:
    """
    Raises a :class:`NotImplementedError` for stubbed writes.

    Parameters
    ----------
    path : Path
        Path to the stubbed file on disk.
    data : JSONData
        Data to write as stubbed file. Should be a list of dictionaries or a
        single dictionary.
    format_name : str
        Human-readable format name.

    Returns
    -------
    int
        The number of rows written to the stubbed file.

    Raises
    ------
    NotImplementedError
        Always, since this is a stub implementation.
    """
    _ = path
    _ = data
    raise NotImplementedError(f'{format_name} write is not implemented yet')
