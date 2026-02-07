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
from .base import ScientificDatasetFileHandlerABC
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


# SECTION: FUNCTIONS ======================================================== #


class SavFile(ScientificDatasetFileHandlerABC):
    """
    Handler implementation for SAV files.
    """

    format = FileFormat.SAV
    dataset_key = 'data'

    def list_datasets(
        self,
        path: Path,
    ) -> list[str]:
        """
        Return available SAV dataset keys.

        Parameters
        ----------
        path : Path
            Path to the SAV file on disk.

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
        dataset = options.dataset if options is not None else None
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
        Read a dataset from SAV at *path*.

        Parameters
        ----------
        path : Path
            Path to the SAV file on disk.
        dataset : str | None, optional
            Dataset selector. SAV supports a single dataset key.
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
                f'SAV supports only dataset key {self.dataset_key!r}',
            )
        pyreadstat = get_dependency('pyreadstat', format_name='SAV')
        frame, _meta = pyreadstat.read_sav(str(path))
        return cast(JSONList, frame.to_dict(orient='records'))

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write *data* to SAV at *path* and return record count.

        Parameters
        ----------
        path : Path
            Path to the SAV file on disk.
        data : JSONData
            Data to write as SAV. Should be a list of dictionaries or a
            single dictionary.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        int
            The number of rows written to the SAV file.
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
        Write one dataset to SAV at *path*.

        Parameters
        ----------
        path : Path
            Path to the SAV file on disk.
        data : JSONData
            Dataset payload to write.
        dataset : str | None, optional
            Dataset selector. SAV supports a single dataset key.
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
        """
        _ = options
        if dataset is not None and dataset != self.dataset_key:
            raise ValueError(
                f'SAV supports only dataset key {self.dataset_key!r}',
            )

        records = normalize_records(data, 'SAV')
        if not records:
            return 0

        pyreadstat = get_dependency('pyreadstat', format_name='SAV')
        pandas = get_pandas('SAV')
        ensure_parent_dir(path)
        frame = pandas.DataFrame.from_records(records)
        pyreadstat.write_sav(frame, str(path))
        return len(records)


# SECTION: INTERNAL CONSTANTS ============================================== #


_SAV_HANDLER = SavFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Read SAV content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the SAV file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the SAV file.
    """
    return _SAV_HANDLER.read(coerce_path(path))


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Write *data* to SAV at *path* and return record count.

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
    return _SAV_HANDLER.write(coerce_path(path), data)
