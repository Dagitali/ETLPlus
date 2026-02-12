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
    'XptFile',
    # Functions
    'read',
    'write',
]


# SECTION: CLASSES ========================================================== #


class XptFile(SingleDatasetScientificFileHandlerABC):
    """
    Handler implementation for XPT files.
    """

    # -- Class Attributes -- #

    format = FileFormat.XPT
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
        Read and return one dataset from XPT at *path*.

        Parameters
        ----------
        path : Path
            Path to the XPT file on disk.
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

        pandas = get_pandas('XPT')
        pyreadstat = get_dependency('pyreadstat', format_name='XPT')
        reader = getattr(pyreadstat, 'read_xport', None)
        if reader is not None:
            frame, _meta = reader(str(path))
            return records_from_table(frame)
        try:
            frame = pandas.read_sas(path, format='xport')
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
        Write one dataset to XPT at *path* and return record count.

        Parameters
        ----------
        path : Path
            Path to the XPT file on disk.
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
            If "pyreadstat" is not installed with write support.
        """
        dataset = self.resolve_write_dataset(dataset, options=options)
        self.validate_single_dataset_key(dataset)

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


# SECTION: INTERNAL CONSTANTS =============================================== #

_XPT_HANDLER = XptFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONData:
    """
    Deprecated wrapper. Use ``XptFile().read(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the XPT file on disk.

    Returns
    -------
    JSONData
        The structured data read from the XPT file.
    """
    return call_deprecated_module_read(
        path,
        __name__,
        _XPT_HANDLER.read,
    )


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Deprecated wrapper. Use ``XptFile().write(...)`` instead.

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
    return call_deprecated_module_write(
        path,
        data,
        __name__,
        _XPT_HANDLER.write,
    )
