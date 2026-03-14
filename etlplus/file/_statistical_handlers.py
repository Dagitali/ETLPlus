"""
:mod:`etlplus.file._statistical_handlers` module.

Shared frame mixins for statistical tabular handlers (DTA/SAV/XPT/SAS7BDAT).
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


# SECTION: INTERNAL CLASSES ================================================= #


class _PyreadstatFrameMixin:
    """Internal helpers for pyreadstat-backed frame reads and writes."""

    format_name: ClassVar[str]
    pyreadstat_module_name: ClassVar[str] = 'pyreadstat'

    def read_pyreadstat_frame(
        self,
        path: Path,
        *,
        pyreadstat: Any | None,
        method_name: str,
        optional: bool = False,
    ) -> Any | None:
        """
        Read one frame via one pyreadstat reader method.

        Parameters
        ----------
        path : Path
            The path to the file to read.
        pyreadstat : Any | None
            The pyreadstat module to use for reading, if available.
        method_name : str
            The name of the pyreadstat reader method to call.
        optional : bool, optional
            Whether missing reader methods should return ``None`` instead of
            raising. Defaults to ``False``.

        Returns
        -------
        Any | None
            The dataframe-like dataset read from the file, or ``None`` when
            ``optional`` is true and the reader method is unavailable.
        """
        if optional:
            return read_module_frame_if_supported(
                module=pyreadstat,
                method_name=method_name,
                path=path,
            )
        return read_module_frame(
            module=pyreadstat,
            format_name=self.format_name,
            module_name=self.pyreadstat_module_name,
            method_name=method_name,
            path=path,
        )

    def write_pyreadstat_frame(
        self,
        path: Path,
        frame: Any,
        *,
        pyreadstat: Any | None,
        method_name: str,
    ) -> None:
        """
        Write one frame via one required pyreadstat writer method.

        Parameters
        ----------
        path : Path
            The path to the file to write.
        frame : Any
            The dataframe-like dataset to write.
        pyreadstat : Any | None
            The pyreadstat module to use for writing, if available.
        method_name : str
            The name of the pyreadstat writer method to call.
        """
        write_module_frame(
            module=pyreadstat,
            format_name=self.format_name,
            module_name=self.pyreadstat_module_name,
            method_name=method_name,
            frame=frame,
            path=path,
        )


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


class PyreadstatReadWriteFrameMixin(_PyreadstatFrameMixin):
    """
    Shared frame read/write behavior for direct :mod:`pyreadstat` method pairs.
    """

    # -- Class Attributes -- #

    pyreadstat_read_method: ClassVar[str]
    pyreadstat_write_method: ClassVar[str]

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

        Parameters
        ----------
        path : Path
            The path to the file to read.
        pandas : Any
            The pandas module to use for reading.
        pyreadstat : Any | None
            The pyreadstat module to use for reading, if available.
        options : ReadOptions | None, optional
            Additional read options, if available.

        Returns
        -------
        Any
            The dataframe-like dataset read from the file.
        """
        _ = pandas
        _ = options
        frame = self.read_pyreadstat_frame(
            path,
            pyreadstat=pyreadstat,
            method_name=self.pyreadstat_read_method,
        )
        assert frame is not None
        return frame

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
        self.write_pyreadstat_frame(
            path,
            frame,
            pyreadstat=pyreadstat,
            method_name=self.pyreadstat_write_method,
        )


class PyreadstatReadSasFallbackFrameMixin(_PyreadstatFrameMixin):
    """
    Shared read behavior using optional :mod:`pyreadstat` then :mod:`pandas`
    ``read_sas``.
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
            frame = self.read_pyreadstat_frame(
                path,
                pyreadstat=pyreadstat,
                method_name=self.pyreadstat_read_method,
                optional=True,
            )
            if frame is not None:
                return frame
        return read_sas_table(pandas, path, format_hint=self.sas_format_hint)


class PyreadstatRequiredWriteFrameMixin(_PyreadstatFrameMixin):
    """
    Shared write behavior requiring one :mod:`pyreadstat` writer method.
    """

    # -- Class Attributes -- #

    pyreadstat_write_method: ClassVar[str]

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
        self.write_pyreadstat_frame(
            path,
            frame,
            pyreadstat=pyreadstat,
            method_name=self.pyreadstat_write_method,
        )
