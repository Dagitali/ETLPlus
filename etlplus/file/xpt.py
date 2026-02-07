"""
:mod:`etlplus.file.xpt` module.

Helpers for reading/writing SAS Transport (XPT) files.

Notes
-----
- A SAS Transport (XPT) file is a standardized file format used to transfer
    SAS datasets between different systems.
- Common cases:
    - Sharing datasets between different SAS installations.
    - Archiving datasets in a platform-independent format.
    - Importing/exporting data to/from statistical software that supports XPT.
- Rule of thumb:
    - If you need to work with XPT files, use this module for reading
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
    'XptFile',
    # Functions
    'read',
    'write',
]


# SECTION: FUNCTIONS ======================================================== #


class XptFile(ScientificDatasetFileHandlerABC):
    """
    Handler implementation for XPT files.
    """

    format = FileFormat.XPT
    dataset_key = 'data'

    def list_datasets(
        self,
        path: Path,
    ) -> list[str]:
        """
        Return available XPT dataset keys.

        Parameters
        ----------
        path : Path
            Path to the XPT file on disk.

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
        Read XPT content from *path*.

        Parameters
        ----------
        path : Path
            Path to the XPT file on disk.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONList
            The list of dictionaries read from the XPT file.
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
        Read a dataset from XPT at *path*.

        Parameters
        ----------
        path : Path
            Path to the XPT file on disk.
        dataset : str | None, optional
            Dataset selector. XPT supports a single dataset key.
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
                f'XPT supports only dataset key {self.dataset_key!r}',
            )

        pandas = get_pandas('XPT')
        pyreadstat = get_dependency('pyreadstat', format_name='XPT')
        reader = getattr(pyreadstat, 'read_xport', None)
        if reader is not None:
            frame, _meta = reader(str(path))
            return cast(JSONList, frame.to_dict(orient='records'))
        try:
            frame = pandas.read_sas(path, format='xport')
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
        Write *data* to XPT file at *path* and return record count.

        Parameters
        ----------
        path : Path
            Path to the XPT file on disk.
        data : JSONData
            Data to write as XPT file. Should be a list of dictionaries or a
            single dictionary.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        int
            The number of rows written to the XPT file.
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
        Write one dataset to XPT at *path*.

        Parameters
        ----------
        path : Path
            Path to the XPT file on disk.
        data : JSONData
            Dataset payload to write.
        dataset : str | None, optional
            Dataset selector. XPT supports a single dataset key.
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
            If "pyreadstat" is not installed with write support.
        """
        _ = options
        if dataset is not None and dataset != self.dataset_key:
            raise ValueError(
                f'XPT supports only dataset key {self.dataset_key!r}',
            )

        records = normalize_records(data, 'XPT')
        if not records:
            return 0

        pandas = get_pandas('XPT')
        pyreadstat = get_dependency('pyreadstat', format_name='XPT')
        writer = getattr(pyreadstat, 'write_xport', None)
        if writer is None:
            raise ImportError(
                'XPT write support requires "pyreadstat" with write_xport().',
            )

        ensure_parent_dir(path)
        frame = pandas.DataFrame.from_records(records)
        writer(frame, str(path))
        return len(records)


# SECTION: INTERNAL CONSTANTS ============================================== #


_XPT_HANDLER = XptFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Read XPT content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the XPT file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the XPT file.
    """
    return _XPT_HANDLER.read(coerce_path(path))


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Write *data* to XPT file at *path* and return record count.

    Parameters
    ----------
    path : StrPath
        Path to the XPT file on disk.
    data : JSONData
        Data to write as XPT file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the XPT file.
    """
    return _XPT_HANDLER.write(coerce_path(path), data)
