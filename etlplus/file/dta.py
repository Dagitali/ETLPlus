"""
:mod:`etlplus.file.dta` module.

Helpers for reading/writing Stata (DTA) files.

Notes
-----
- A DTA file is a proprietary binary format created by Stata to store datasets
    with variables, labels, and data types.
- Common cases:
    - Statistical analysis workflows.
    - Data sharing in research environments.
    - Interchange between Stata and other analytics tools.
- Rule of thumb:
    - If the file follows the DTA specification, use this module for reading
        and writing.
"""

from __future__ import annotations

from pathlib import Path
from typing import cast

from ..types import JSONData
from ..types import JSONList
from ..types import StrPath
from ._imports import get_dependency
from ._imports import get_pandas
from ._io import coerce_path
from ._io import ensure_parent_dir
from ._io import normalize_records
from .base import ReadOptions
from .base import SingleDatasetScientificFileHandlerABC
from .base import WriteOptions
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'DtaFile',
    # Functions
    'read',
    'write',
]


# SECTION: CLASSES ========================================================== #


class DtaFile(SingleDatasetScientificFileHandlerABC):
    """
    Handler implementation for DTA files.
    """

    # -- Class Attributes -- #

    format = FileFormat.DTA
    dataset_key = 'data'

    # -- Instance Methods -- #

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read DTA content from *path*.

        Parameters
        ----------
        path : Path
            Path to the DTA file on disk.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONList
            The list of dictionaries read from the DTA file.
        """
        dataset = self.dataset_from_read_options(options)
        return cast(
            JSONList,
            self.read_dataset(path, dataset=dataset, options=options),
        )

    def read_dataset(
        self,
        path: Path,
        *,
        dataset: str | None = None,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read and return one dataset from DTA at *path*.

        Parameters
        ----------
        path : Path
            Path to the DTA file on disk.
        dataset : str | None, optional
            Dataset selector. Use the default dataset key or ``None``.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONList
            Parsed records.
        """
        _ = options
        self.validate_single_dataset_key(dataset)
        get_dependency('pyreadstat', format_name='DTA')
        pandas = get_pandas('DTA')
        frame = pandas.read_stata(path)
        return cast(JSONList, frame.to_dict(orient='records'))

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write *data* to DTA file at *path* and return record count.

        Parameters
        ----------
        path : Path
            Path to the DTA file on disk.
        data : JSONData
            Data to write as DTA file. Should be a list of dictionaries or a
            single dictionary.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        int
            The number of rows written to the DTA file.
        """
        dataset = self.dataset_from_write_options(options)
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
        Write one dataset to DTA at *path* and return record count.

        Parameters
        ----------
        path : Path
            Path to the DTA file on disk.
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
        """
        _ = options
        self.validate_single_dataset_key(dataset)

        records = normalize_records(data, 'DTA')
        if not records:
            return 0

        get_dependency('pyreadstat', format_name='DTA')
        pandas = get_pandas('DTA')
        ensure_parent_dir(path)
        frame = pandas.DataFrame.from_records(records)
        frame.to_stata(path, write_index=False)
        return len(records)


# SECTION: INTERNAL CONSTANTS ============================================== #


_DTA_HANDLER = DtaFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Read DTA content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the DTA file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the DTA file.
    """
    return _DTA_HANDLER.read(coerce_path(path))


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Write *data* to DTA file at *path* and return record count.

    Parameters
    ----------
    path : StrPath
        Path to the DTA file on disk.
    data : JSONData
        Data to write as DTA file. Should be a list of dictionaries or a single
        dictionary.

    Returns
    -------
    int
        The number of rows written to the DTA file.
    """
    return _DTA_HANDLER.write(coerce_path(path), data)
