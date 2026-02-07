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
from ..types import JSONDict
from ..types import StrPath
from ._imports import get_dependency
from ._imports import get_pandas
from ._io import coerce_path
from ._io import ensure_parent_dir
from ._io import normalize_records
from ._r import coerce_r_object
from .base import ReadOptions
from .base import ScientificDatasetFileHandlerABC
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


# SECTION: FUNCTIONS ======================================================== #


class RdsFile(ScientificDatasetFileHandlerABC):
    """
    Handler implementation for RDS files.
    """

    # -- Class Attributes -- #

    format = FileFormat.RDS
    dataset_key = 'data'

    # -- Instance Methods -- #

    def list_datasets(
        self,
        path: Path,
    ) -> list[str]:
        """
        Return available dataset keys in an RDS container.

        Parameters
        ----------
        path : Path
            Path to the RDS file on disk.

        Returns
        -------
        list[str]
            Available dataset keys.
        """
        pyreadr = get_dependency('pyreadr', format_name='RDS')
        result = pyreadr.read_r(str(path))
        if not result:
            return [self.dataset_key]
        return [str(key) for key in result]

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
        dataset = options.dataset if options is not None else None
        return self.read_dataset(path, dataset=dataset, options=options)

    def read_dataset(
        self,
        path: Path,
        *,
        dataset: str | None = None,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Read one dataset from RDS at *path*.

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
        _ = options
        pyreadr = get_dependency('pyreadr', format_name='RDS')
        pandas = get_pandas('RDS')
        result = pyreadr.read_r(str(path))
        if not result:
            return []

        if dataset is not None:
            if dataset in result:
                return coerce_r_object(result[dataset], pandas)
            if dataset == self.dataset_key and len(result) == 1:
                value = next(iter(result.values()))
                return coerce_r_object(value, pandas)
            raise ValueError(f'RDS dataset {dataset!r} not found')

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
        dataset = options.dataset if options is not None else None
        return self.write_dataset(
            path,
            data,
            dataset=dataset,
            options=options,
        )

    def write_dataset(
        self,
        path: Path,
        data: JSONData,
        *,
        dataset: str | None = None,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write one dataset to RDS at *path*.

        Parameters
        ----------
        path : Path
            Path to the RDS file on disk.
        data : JSONData
            Dataset payload to write.
        dataset : str | None, optional
            Dataset selector. RDS supports a single stored object.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        int
            Number of records written.

        Raises
        ------
        ValueError
            If *dataset* is provided and not supported.
        ImportError
            If "pyreadr" is not installed with write support.
        """
        _ = options
        if dataset is not None and dataset != self.dataset_key:
            raise ValueError(
                f'RDS supports only dataset key {self.dataset_key!r}',
            )

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


# SECTION: INTERNAL CONSTANTS ============================================== #


_RDS_HANDLER = RdsFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONData:
    """
    Read RDS content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the RDS file on disk.

    Returns
    -------
    JSONData
        The structured data read from the RDS file.
    """
    return _RDS_HANDLER.read(coerce_path(path))


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Write *data* to RDS file at *path* and return record count.

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
    return _RDS_HANDLER.write(coerce_path(path), data)
