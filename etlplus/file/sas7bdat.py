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
from typing import cast

from ..types import JSONData
from ..types import JSONList
from ..types import StrPath
from . import stub
from ._imports import get_dependency
from ._imports import get_pandas
from ._io import coerce_path
from .base import FileHandlerABC
from .base import ReadOptions
from .base import WriteOptions
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'Sas7bdatFile',
    # Functions
    'read',
    'write',
]


# SECTION: FUNCTIONS ======================================================== #


class Sas7bdatFile(FileHandlerABC):
    """
    Handler implementation for SAS7BDAT files.
    """

    format = FileFormat.SAS7BDAT
    category = 'statistical_dataset'

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read SAS7BDAT content from *path*.

        Parameters
        ----------
        path : Path
            Path to the SAS7BDAT file on disk.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONList
            The list of dictionaries read from the SAS7BDAT file.
        """
        _ = options
        get_dependency('pyreadstat', format_name='SAS7BDAT')
        pandas = get_pandas('SAS7BDAT')
        try:
            frame = pandas.read_sas(path, format='sas7bdat')
        except TypeError:
            frame = pandas.read_sas(path)
        return cast(JSONList, frame.to_dict(orient='records'))

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write *data* to SAS7BDAT file at *path* and return record count.

        Parameters
        ----------
        path : Path
            Path to the SAS7BDAT file on disk.
        data : JSONData
            Data to write as SAS7BDAT file. Should be a list of dictionaries or
            a single dictionary.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        int
            The number of rows written to the SAS7BDAT file.
        """
        _ = options
        return stub.write(path, data, format_name='SAS7BDAT')


# SECTION: INTERNAL CONSTANTS ============================================== #


_SAS7BDAT_HANDLER = Sas7bdatFile()


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
    return _SAS7BDAT_HANDLER.read(coerce_path(path))


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
    return _SAS7BDAT_HANDLER.write(coerce_path(path), data)
