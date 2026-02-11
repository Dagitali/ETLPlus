"""
:mod:`etlplus.file.vm` module.

Stub helpers for reading/writing Apache Velocity (VM) template files (not
implemented yet).

Notes
-----
- A VM file is a text file used for generating HTML or other text formats
    by combining templates with data.
- Common cases:
    - HTML templates.
    - Email templates.
    - Configuration files.
- Rule of thumb:
    - If you need to work with Apache Velocity template files, use this module
        for reading and writing.
"""

from __future__ import annotations

from ..types import JSONData
from ..types import JSONList
from ..types import StrPath
from ._io import coerce_path
from ._stub_categories import StubTemplateFileHandlerABC
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'VmFile',
    # Functions
    'read',
    'write',
]


# SECTION: CLASSES ========================================================== #


class VmFile(StubTemplateFileHandlerABC):
    """
    Stub handler implementation for VM files.
    """

    # -- Class Attributes -- #

    format = FileFormat.VM
    template_engine = 'velocity'

    # -- Instance Methods -- #

    # Inherits read() and write() from StubTemplateFileHandlerABC.


# SECTION: INTERNAL CONSTANTS =============================================== #


_VM_HANDLER = VmFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Read and return VM content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the VM file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the VM file.
    """
    return _VM_HANDLER.read(coerce_path(path))


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Write *data* to VM at *path* and return record count.

    Parameters
    ----------
    path : StrPath
        Path to the VM file on disk.
    data : JSONData
        Data to write as VM file. Should be a list of dictionaries or a single
        dictionary.

    Returns
    -------
    int
        The number of rows written to the VM file.
    """
    return _VM_HANDLER.write(coerce_path(path), data)
