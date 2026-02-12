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
from ..types import StrPath
from ._imports import get_dependency
from ._imports import get_pandas
from ._io import call_deprecated_module_read
from ._io import call_deprecated_module_write
from ._io import ensure_parent_dir
from ._io import normalize_records
from ._r import coerce_r_result
from ._r import list_r_dataset_keys
from .base import ReadOptions
from .base import ScientificDatasetFileHandlerABC
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


# SECTION: CLASSES ========================================================== #


class RdaFile(ScientificDatasetFileHandlerABC):
    """
    Handler implementation for RDA files.
    """

    # -- Class Attributes -- #

    format = FileFormat.RDA
    dataset_key = 'data'

    # -- Instance Methods -- #

    def list_datasets(
        self,
        path: Path,
    ) -> list[str]:
        """
        Return available dataset keys in an RDA container.

        Parameters
        ----------
        path : Path
            Path to the RDA file on disk.

        Returns
        -------
        list[str]
            Available dataset keys.
        """
        pyreadr = get_dependency('pyreadr', format_name=self.format_name)
        result = pyreadr.read_r(str(path))
        return list_r_dataset_keys(
            result,
            default_key=self.dataset_key,
        )

    def read_dataset(
        self,
        path: Path,
        *,
        dataset: str | None = None,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Read one dataset (or all datasets) from RDA at *path*.

        Parameters
        ----------
        path : Path
            Path to the RDA file on disk.
        dataset : str | None, optional
            Dataset key to select. If omitted, all objects are returned.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONData
            Parsed dataset payload.
        """
        format_name = self.format_name
        dataset = self.resolve_read_dataset(dataset, options=options)
        pyreadr = get_dependency('pyreadr', format_name=format_name)
        pandas = get_pandas(format_name)
        result = pyreadr.read_r(str(path))
        return coerce_r_result(
            result,
            dataset=dataset,
            dataset_key=self.dataset_key,
            format_name=format_name,
            pandas=pandas,
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
        Write one dataset to RDA at *path* and return record count.

        Parameters
        ----------
        path : Path
            Path to the RDA file on disk.
        data : JSONData
            Dataset payload to write.
        dataset : str | None, optional
            Target dataset key. Defaults to :attr:`dataset_key`.
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
        format_name = self.format_name
        dataset = self.resolve_write_dataset(dataset, options=options)
        pyreadr = get_dependency('pyreadr', format_name=format_name)
        pandas = get_pandas(format_name)
        records = normalize_records(data, format_name)
        frame = pandas.DataFrame.from_records(records)
        count = len(records)
        target_dataset = dataset if dataset is not None else self.dataset_key

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
            writer(str(path), frame, df_name=target_dataset)
        except TypeError:
            writer(str(path), frame)
        return count


# SECTION: INTERNAL CONSTANTS =============================================== #

_RDA_HANDLER = RdaFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONData:
    """
    Deprecated wrapper. Use ``RdaFile().read(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the RDA file on disk.

    Returns
    -------
    JSONData
        The structured data read from the RDA file.
    """
    return call_deprecated_module_read(
        path,
        __name__,
        _RDA_HANDLER.read,
    )


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Deprecated wrapper. Use ``RdaFile().write(...)`` instead.

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
    return call_deprecated_module_write(
        path,
        data,
        __name__,
        _RDA_HANDLER.write,
    )
