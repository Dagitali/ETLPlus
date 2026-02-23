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
from typing import Any

from ._imports import get_dependency
from ._imports import get_pandas
from ._scientific_handlers import SingleDatasetTabularScientificReadWriteMixin
from .base import ReadOptions
from .base import WriteOptions
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'SavFile',
]


# SECTION: CLASSES ========================================================== #


class SavFile(SingleDatasetTabularScientificReadWriteMixin):
    """
    Handler implementation for SAV files.
    """

    # -- Class Attributes -- #

    format = FileFormat.SAV
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
        Read and return one dataframe-like dataset from SAV.

        Parameters
        ----------
        path : Path
            Path to the SAV file to read.
        pandas : Any
            The pandas module, passed via dependency injection.
        pyreadstat : Any | None
            The pyreadstat module, passed via dependency injection when
            required by the mixin. Will be None if not required.
        options : ReadOptions | None
            Optional read options. May be ignored by this handler.

        Returns
        -------
        Any
            The resulting dataframe-like dataset.

        Raises
        ------
        RuntimeError
            If the pyreadstat dependency is required but not provided.
        """
        _ = pandas
        _ = options
        if pyreadstat is None:  # pragma: no cover - guarded by mixin flag
            raise RuntimeError(
                'pyreadstat dependency is required for SAV read',
            )
        frame, _meta = pyreadstat.read_sav(str(path))
        return frame

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
        Write one dataframe-like dataset to SAV.

        Parameters
        ----------
        path : Path
            Path to the SAV file to write.
        frame : Any
            The dataframe-like dataset to write.
        pandas : Any
            The pandas module, passed via dependency injection.
        pyreadstat : Any | None
            The pyreadstat module, passed via dependency injection when
            required by the mixin. Will be None if not required.
        options : WriteOptions | None
            Optional write options. May be ignored by this handler.

        Raises
        ------
        RuntimeError
            If the pyreadstat dependency is required but not provided.
        """
        _ = pandas
        _ = options
        if pyreadstat is None:  # pragma: no cover - guarded by mixin flag
            raise RuntimeError(
                'pyreadstat dependency is required for SAV write',
            )
        pyreadstat.write_sav(frame, str(path))
