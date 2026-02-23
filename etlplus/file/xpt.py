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
from typing import Any

from ._imports import get_dependency
from ._imports import get_pandas
from ._io import read_sas_table
from ._scientific_handlers import SingleDatasetTabularScientificReadWriteMixin
from .base import ReadOptions
from .base import WriteOptions
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'XptFile',
]


# SECTION: CLASSES ========================================================== #


class XptFile(SingleDatasetTabularScientificReadWriteMixin):
    """
    Handler implementation for XPT files.
    """

    # -- Class Attributes -- #

    format = FileFormat.XPT
    requires_pyreadstat_for_read = True
    requires_pyreadstat_for_write = True

    # -- Instance Methods -- #

    def read_frame(
        self,
        path: Path,
        *,
        pandas: Any,
        pyreadstat: Any | None,
        options: ReadOptions | None = None,
    ) -> Any:
        """
        Read and return one dataframe-like dataset from XPT.
        """
        _ = options
        reader = getattr(pyreadstat, 'read_xport', None)
        if reader is not None:
            frame, _meta = reader(str(path))
            return frame
        return read_sas_table(pandas, path, format_hint='xport')

    def resolve_pandas(self) -> Any:
        """
        Return pandas using module-level dependency resolution.
        """
        return get_pandas(self.format_name)

    def resolve_pyreadstat(self) -> Any:
        """
        Return pyreadstat using module-level dependency resolution.
        """
        return get_dependency('pyreadstat', format_name=self.format_name)

    def write_frame(
        self,
        path: Path,
        frame: Any,
        *,
        pandas: Any,
        pyreadstat: Any | None,
        options: WriteOptions | None = None,
    ) -> None:
        """
        Write one dataframe-like dataset to XPT.
        """
        _ = pandas
        _ = options
        writer = getattr(pyreadstat, 'write_xport', None)
        if writer is None:
            raise ImportError(
                'XPT write support requires "pyreadstat" with write_xport().',
            )
        writer(frame, str(path))
