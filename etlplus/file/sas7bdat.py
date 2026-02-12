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

from ..types import JSONData
from ..types import JSONList
from ..types import StrPath
from ._imports import get_dependency
from ._imports import get_pandas
from ._io import call_deprecated_module_read
from ._io import call_deprecated_module_write
from ._io import records_from_table
from .base import ReadOnlyFileHandlerABC
from .base import ReadOptions
from .base import SingleDatasetScientificFileHandlerABC
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


# SECTION: CLASSES ========================================================== #


class Sas7bdatFile(
    ReadOnlyFileHandlerABC,
    SingleDatasetScientificFileHandlerABC,
):
    """
    Read-only handler implementation for SAS7BDAT files.
    """

    # -- Class Attributes -- #

    format = FileFormat.SAS7BDAT
    dataset_key = 'data'

    # -- Instance Methods -- #

    def read_dataset(
        self,
        path: Path,
        *,
        dataset: str | None = None,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read and return one dataset from SAS7BDAT at *path*.

        Parameters
        ----------
        path : Path
            Path to the SAS7BDAT file on disk.
        dataset : str | None, optional
            Dataset selector. Use the default dataset key or ``None``.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONList
            Parsed records.
        """
        dataset = self.resolve_read_dataset(dataset, options=options)
        self.validate_single_dataset_key(dataset)
        get_dependency('pyreadstat', format_name='SAS7BDAT')
        pandas = get_pandas('SAS7BDAT')
        try:
            frame = pandas.read_sas(path, format='sas7bdat')
        except TypeError:
            frame = pandas.read_sas(path)
        return records_from_table(frame)

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
        dataset = self.resolve_write_dataset(dataset, options=options)
        self.validate_single_dataset_key(dataset)
        return self.write(path, data, options=options)


# SECTION: INTERNAL CONSTANTS =============================================== #

_SAS7BDAT_HANDLER = Sas7bdatFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONData:
    """
    Deprecated wrapper. Use ``Sas7bdatFile().read(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the SAS7BDAT file on disk.

    Returns
    -------
    JSONData
        The structured data read from the SAS7BDAT file.
    """
    return call_deprecated_module_read(
        path,
        __name__,
        _SAS7BDAT_HANDLER.read,
    )


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Deprecated wrapper. Use ``Sas7bdatFile().write(...)`` instead.

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
    return call_deprecated_module_write(
        path,
        data,
        __name__,
        _SAS7BDAT_HANDLER.write,
    )
