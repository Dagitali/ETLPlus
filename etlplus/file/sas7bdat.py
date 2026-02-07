"""
:mod:`etlplus.file.sas7bdat` module.

Helpers for reading SAS (SAS7BDAT) data files.

Notes
-----
- A SAS7BDAT file is a proprietary binary file format created by SAS to store
    datasets, including variables, labels, and data types.
- Common cases:
    - Statistical analysis pipelines.
    - Data exchange with SAS tooling.
- Rule of thumb:
    - If the file follows the SAS7BDAT specification, use this module for
        reading.
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
from .base import ReadOnlyFileHandlerABC
from .base import ReadOptions
from .base import ScientificDatasetFileHandlerABC
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


class Sas7bdatFile(ReadOnlyFileHandlerABC, ScientificDatasetFileHandlerABC):
    """
    Read-only handler implementation for SAS7BDAT files.
    """

    format = FileFormat.SAS7BDAT
    dataset_key = 'data'

    def list_datasets(
        self,
        path: Path,
    ) -> list[str]:
        """
        Return available SAS7BDAT dataset keys.

        Parameters
        ----------
        path : Path
            Path to the SAS7BDAT file on disk.

        Returns
        -------
        list[str]
            Available dataset keys.
        """
        _ = path
        return [self.dataset_key]

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
        dataset = options.dataset if options is not None else None
        return self.read_dataset(path, dataset=dataset, options=options)

    def read_dataset(
        self,
        path: Path,
        *,
        dataset: str | None = None,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read one dataset from SAS7BDAT at *path*.

        Parameters
        ----------
        path : Path
            Path to the SAS7BDAT file on disk.
        dataset : str | None, optional
            Dataset selector. SAS7BDAT supports a single dataset key.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONList
            Parsed records.

        Raises
        ------
        ValueError
            If *dataset* is provided and not supported.
        """
        _ = options
        if dataset is not None and dataset != self.dataset_key:
            raise ValueError(
                f'SAS7BDAT supports only dataset key {self.dataset_key!r}',
            )
        get_dependency('pyreadstat', format_name='SAS7BDAT')
        pandas = get_pandas('SAS7BDAT')
        try:
            frame = pandas.read_sas(path, format='sas7bdat')
        except TypeError:
            frame = pandas.read_sas(path)
        return cast(JSONList, frame.to_dict(orient='records'))

    def write_dataset(
        self,
        path: Path,
        data: JSONData,
        *,
        dataset: str | None = None,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Reject writes for SAS7BDAT while preserving scientific dataset
        contract.
        """
        _ = dataset
        return self.write(path, data, options=options)


# SECTION: INTERNAL CONSTANTS ============================================== #


_SAS7BDAT_HANDLER = Sas7bdatFile()


# SECTION: FUNCTIONS ======================================================== #


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
        Never returns normally.
    """
    return _SAS7BDAT_HANDLER.write(coerce_path(path), data)
