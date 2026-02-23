"""
:mod:`etlplus.file._statistical_handlers` module.

Shared mixins for statistical tabular handlers (DTA/SAV/XPT/SAS7BDAT).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from typing import ClassVar

from ._io import read_sas_table
from ._module_callables import read_module_frame
from ._module_callables import read_module_frame_if_supported
from ._module_callables import write_module_frame
from .base import ReadOptions
from .base import WriteOptions

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'PandasStataReadWriteFrameMixin',
    'PyreadstatReadWriteFrameMixin',
    'PyreadstatRequiredWriteFrameMixin',
    'PyreadstatReadSasFallbackFrameMixin',
]


# SECTION: CLASSES ========================================================== #


class PandasStataReadWriteFrameMixin:
    """
    Shared frame read/write behavior for pandas ``read_stata``/``to_stata``.
    """

    # -- Class Attributes -- #

    pandas_read_method: ClassVar[str] = 'read_stata'
    pandas_write_method: ClassVar[str] = 'to_stata'
    pandas_write_kwargs: ClassVar[tuple[tuple[str, object], ...]] = (
        ('write_index', False),
    )

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
        Read one dataframe-like dataset using pandas Stata I/O.

        Parameters
        ----------
        path : Path
            The path to the Stata file to read.
        pandas : Any
            The pandas module to use for reading.
        pyreadstat : Any | None, optional
            The pyreadstat module to use for reading, if available.
        options : ReadOptions | None, optional
            Additional read options, if available.

        Returns
        -------
        Any
            The dataframe-like dataset read from the Stata file.
        """
        _ = pyreadstat
        _ = options
        return getattr(pandas, self.pandas_read_method)(path)

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
        Write one dataframe-like dataset using pandas Stata I/O.

        Parameters
        ----------
        path : Path
            The path to the Stata file to write.
        frame : Any
            The dataframe-like dataset to write.
        pandas : Any
            The pandas module to use for writing.
        pyreadstat : Any | None, optional
            The pyreadstat module to use for writing, if available.
        options : WriteOptions | None, optional
            Additional write options, if available.
        """
        _ = pandas
        _ = pyreadstat
        _ = options
        getattr(frame, self.pandas_write_method)(
            path,
            **dict(self.pandas_write_kwargs),
        )


class PyreadstatReadWriteFrameMixin:
    """
    Shared frame read/write behavior for direct ``pyreadstat`` method pairs.
    """

    # -- Class Attributes -- #

    pyreadstat_read_method: ClassVar[str]
    pyreadstat_write_method: ClassVar[str]
    format_name: ClassVar[str]

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
        Read one dataframe-like dataset using ``pyreadstat``.
        """
        _ = pandas
        _ = options
        return read_module_frame(
            module=pyreadstat,
            format_name=self.format_name,
            module_name='pyreadstat',
            method_name=self.pyreadstat_read_method,
            path=path,
        )

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
        Write one dataframe-like dataset using ``pyreadstat``.
        """
        _ = pandas
        _ = options
        write_module_frame(
            module=pyreadstat,
            format_name=self.format_name,
            module_name='pyreadstat',
            method_name=self.pyreadstat_write_method,
            frame=frame,
            path=path,
        )


class PyreadstatReadSasFallbackFrameMixin:
    """
    Shared frame read behavior using optional pyreadstat then pandas fallback.
    """

    # -- Class Attributes -- #

    pyreadstat_read_method: ClassVar[str | None] = None
    sas_format_hint: ClassVar[str]

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
        Read one dataframe-like dataset with pyreadstat fallback behavior.

        Parameters
        ----------
        path : Path
            The path to the SAS file to read.
        pandas : Any
            The pandas module to use for reading.
        pyreadstat : Any | None, optional
            The pyreadstat module to use for reading, if available.
        options : ReadOptions | None, optional
            Additional read options, if available.

        Returns
        -------
        Any
            The dataframe-like dataset read from the SAS file.
        """
        _ = options
        if self.pyreadstat_read_method is not None:
            if (
                frame := read_module_frame_if_supported(
                    module=pyreadstat,
                    method_name=self.pyreadstat_read_method,
                    path=path,
                )
            ) is not None:
                return frame
        return read_sas_table(pandas, path, format_hint=self.sas_format_hint)


class PyreadstatRequiredWriteFrameMixin:
    """
    Shared frame write behavior requiring one ``pyreadstat`` writer method.
    """

    # -- Class Attributes -- #

    pyreadstat_write_method: ClassVar[str]
    format_name: ClassVar[str]

    # -- Instance Methods -- #

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
        Write one dataframe-like dataset via a required pyreadstat method.

        Parameters
        ----------
        path : Path
            The path to the file to write.
        frame : Any
            The dataframe-like dataset to write.
        pandas : Any
            The pandas module to use for writing.
        pyreadstat : Any | None, optional
            The pyreadstat module to use for writing, if available.
        options : WriteOptions | None, optional
            Additional write options, if available.
        """
        _ = pandas
        _ = options
        write_module_frame(
            module=pyreadstat,
            format_name=self.format_name,
            module_name='pyreadstat',
            method_name=self.pyreadstat_write_method,
            frame=frame,
            path=path,
        )
