"""
:mod:`etlplus.file.rds` module.

Helpers for reading/writing R (RDS) data files.

Notes
-----
- An RDS file is a binary file format used by R to store a single R object,
    such as a data frame, list, or vector.
- Common cases:
    - Storing R objects for later use.
    - Sharing R data between users.
    - Loading R data into Python for analysis.
- Rule of thumb:
    - If the file follows the RDS specification, use this module for reading
        and writing.
"""

from __future__ import annotations

from pathlib import Path

from ..types import JSONData
from ..types import StrPath
from ._imports import get_dependency
from ._imports import get_pandas
from ._io import call_deprecated_module_read
from ._io import call_deprecated_module_write
from ._io import ensure_parent_dir
from ._io import normalize_records
from ._r import coerce_r_result
from .base import ReadOptions
from .base import SingleDatasetScientificFileHandlerABC
from .base import WriteOptions
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'RdsFile',
    # Functions
    'read',
    'write',
]


# SECTION: CLASSES ========================================================== #


class RdsFile(SingleDatasetScientificFileHandlerABC):
    """
    Handler implementation for RDS files.
    """

    # -- Class Attributes -- #

    format = FileFormat.RDS
    dataset_key = 'data'

    # -- Instance Methods -- #

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Read RDS content from *path*.

        Parameters
        ----------
        path : Path
            Path to the RDS file on disk.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONData
            The structured data read from the RDS file.
        """
        return self.read_dataset(path, options=options)

    def read_dataset(
        self,
        path: Path,
        *,
        dataset: str | None = None,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Read and return one dataset from RDS at *path*.

        Parameters
        ----------
        path : Path
            Path to the RDS file on disk.
        dataset : str | None, optional
            Dataset key to select. If omitted, default behavior is preserved.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONData
            Parsed dataset payload.

        Raises
        ------
        ValueError
            If an explicit dataset key is not present.
        """
        dataset = self.resolve_read_dataset(dataset, options=options)
        pyreadr = get_dependency('pyreadr', format_name='RDS')
        pandas = get_pandas('RDS')
        result = pyreadr.read_r(str(path))
        return coerce_r_result(
            result,
            dataset=dataset,
            dataset_key=self.dataset_key,
            format_name='RDS',
            pandas=pandas,
        )

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write *data* to RDS file at *path* and return record count.

        Parameters
        ----------
        path : Path
            Path to the RDS file on disk.
        data : JSONData
            Data to write as RDS file. Should be a list of dictionaries or a
            single dictionary.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        int
            The number of rows written to the RDS file.
        """
        return self.write_dataset(path, data, options=options)

    def write_dataset(
        self,
        path: Path,
        data: JSONData,
        *,
        dataset: str | None = None,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write one dataset to RDS at *path* and return record count.

        Parameters
        ----------
        path : Path
            Path to the RDS file on disk.
        data : JSONData
            Dataset payload to write.
        dataset : str | None, optional
            Dataset selector. Use the default dataset key or ``None``.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        int
            Number of records written.

        Raises
        ------
        ImportError
            If "pyreadr" is not installed with write support.
        """
        dataset = self.resolve_write_dataset(dataset, options=options)
        self.validate_single_dataset_key(dataset)

        pyreadr = get_dependency('pyreadr', format_name='RDS')
        pandas = get_pandas('RDS')
        records = normalize_records(data, 'RDS')
        frame = pandas.DataFrame.from_records(records)
        count = len(records)

        writer = getattr(pyreadr, 'write_rds', None)
        if writer is None:
            raise ImportError(
                'RDS write support requires "pyreadr" with write_rds().',
            )

        ensure_parent_dir(path)
        writer(str(path), frame)
        return count


# SECTION: INTERNAL CONSTANTS =============================================== #

_RDS_HANDLER = RdsFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONData:
    """
    Deprecated wrapper. Use ``RdsFile().read(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the RDS file on disk.

    Returns
    -------
    JSONData
        The structured data read from the RDS file.
    """
    return call_deprecated_module_read(
        path,
        __name__,
        _RDS_HANDLER.read,
    )


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Deprecated wrapper. Use ``RdsFile().write(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the RDS file on disk.
    data : JSONData
        Data to write as RDS file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the RDS file.
    """
    return call_deprecated_module_write(
        path,
        data,
        __name__,
        _RDS_HANDLER.write,
    )
