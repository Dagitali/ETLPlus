"""
:mod:`etlplus.file.rda` module.

Helpers for reading/writing RData workspace/object bundle (RDA) files.

Notes
-----
- A RDA file is a binary file format used by R to store workspace objects,
    including data frames, lists, and other R objects.
- Common cases:
    - Storing R data objects for later use.
    - Sharing R datasets between users.
    - Loading R data into Python for analysis.
- Rule of thumb:
    - If the file follows the RDA specification, use this module for reading
        and writing.
"""

from __future__ import annotations

from pathlib import Path

from ..types import JSONData
from ..types import JSONDict
from ..types import StrPath
from ._imports import get_dependency
from ._imports import get_pandas
from ._io import coerce_path
from ._io import ensure_parent_dir
from ._io import normalize_records
from ._r import coerce_r_object
from .base import FileHandlerABC
from .base import ReadOptions
from .base import WriteOptions
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'RdaFile',
    # Functions
    'read',
    'write',
]


# SECTION: FUNCTIONS ======================================================== #


class RdaFile(FileHandlerABC):
    """
    Handler implementation for RDA files.
    """

    format = FileFormat.RDA
    category = 'statistical_dataset'

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Read RDA content from *path*.

        Parameters
        ----------
        path : Path
            Path to the RDA file on disk.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONData
            The structured data read from the RDA file.
        """
        _ = options
        pyreadr = get_dependency('pyreadr', format_name='RDA')
        pandas = get_pandas('RDA')
        result = pyreadr.read_r(str(path))
        if not result:
            return []
        if len(result) == 1:
            value = next(iter(result.values()))
            return coerce_r_object(value, pandas)
        payload: JSONDict = {}
        for key, value in result.items():
            payload[str(key)] = coerce_r_object(value, pandas)
        return payload

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write *data* to RDA file at *path* and return record count.

        Parameters
        ----------
        path : Path
            Path to the RDA file on disk.
        data : JSONData
            Data to write as RDA file. Should be a list of dictionaries or a
            single dictionary.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        int
            The number of rows written to the RDA file.

        Raises
        ------
        ImportError
            If "pyreadr" is not installed with write support.
        """
        _ = options
        pyreadr = get_dependency('pyreadr', format_name='RDA')
        pandas = get_pandas('RDA')
        records = normalize_records(data, 'RDA')
        frame = pandas.DataFrame.from_records(records)
        count = len(records)

        writer = getattr(pyreadr, 'write_rdata', None) or getattr(
            pyreadr,
            'write_rda',
            None,
        )
        if writer is None:
            raise ImportError(
                'RDA write support requires "pyreadr" with write_rdata().',
            )

        ensure_parent_dir(path)
        try:
            writer(str(path), frame, df_name='data')
        except TypeError:
            writer(str(path), frame)
        return count


# SECTION: INTERNAL CONSTANTS ============================================== #


_RDA_HANDLER = RdaFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONData:
    """
    Read RDA content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the RDA file on disk.

    Returns
    -------
    JSONData
        The structured data read from the RDA file.
    """
    return _RDA_HANDLER.read(coerce_path(path))


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Write *data* to RDA file at *path* and return record count.

    Parameters
    ----------
    path : StrPath
        Path to the RDA file on disk.
    data : JSONData
        Data to write as RDA file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the RDA file.
    """
    return _RDA_HANDLER.write(coerce_path(path), data)
