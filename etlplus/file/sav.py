"""
:mod:`etlplus.file.sav` module.

Helpers for reading/writing SPSS (SAV) files.

Notes
-----
- A SAV file is a dataset created by SPSS.
- Common cases:
    - Survey and market research datasets.
    - Statistical analysis workflows.
    - Exchange with SPSS and compatible tools.
- Rule of thumb:
    - If the file follows the SAV specification, use this module for reading
        and writing.
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
from ._io import ensure_parent_dir
from ._io import normalize_records
from ._io import records_from_table
from .base import ReadOptions
from .base import SingleDatasetScientificFileHandlerABC
from .base import WriteOptions
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'SavFile',
    # Functions
    'read',
    'write',
]


# SECTION: CLASSES ========================================================== #


class SavFile(SingleDatasetScientificFileHandlerABC):
    """
    Handler implementation for SAV files.
    """

    # -- Class Attributes -- #

    format = FileFormat.SAV
    dataset_key = 'data'

    # -- Instance Methods -- #

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read SAV content from *path*.

        Parameters
        ----------
        path : Path
            Path to the SAV file on disk.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONList
            The list of dictionaries read from the SAV file.
        """
        return self.read_dataset(path, options=options)

    def read_dataset(
        self,
        path: Path,
        *,
        dataset: str | None = None,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read and return one dataset from SAV at *path*.

        Parameters
        ----------
        path : Path
            Path to the SAV file on disk.
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
        pyreadstat = get_dependency('pyreadstat', format_name='SAV')
        frame, _meta = pyreadstat.read_sav(str(path))
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
        Write one dataset to SAV at *path* and return record count.

        Parameters
        ----------
        path : Path
            Path to the SAV file on disk.
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
        dataset = self.resolve_write_dataset(dataset, options=options)
        self.validate_single_dataset_key(dataset)

        records = normalize_records(data, 'SAV')
        if not records:
            return 0

        pyreadstat = get_dependency('pyreadstat', format_name='SAV')
        pandas = get_pandas('SAV')
        ensure_parent_dir(path)
        frame = pandas.DataFrame.from_records(records)
        pyreadstat.write_sav(frame, str(path))
        return len(records)


# SECTION: INTERNAL CONSTANTS =============================================== #

_SAV_HANDLER = SavFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Deprecated wrapper. Use ``SavFile().read(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the SAV file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the SAV file.
    """
    return call_deprecated_module_read(
        path,
        __name__,
        _SAV_HANDLER.read,
    )


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Deprecated wrapper. Use ``SavFile().write(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the SAV file on disk.
    data : JSONData
        Data to write as SAV. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the SAV file.
    """
    return call_deprecated_module_write(
        path,
        data,
        __name__,
        _SAV_HANDLER.write,
    )
