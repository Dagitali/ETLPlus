"""
:mod:`etlplus.file.core` module.

Shared helpers for reading and writing structured and semi-structured data
files.
"""

from __future__ import annotations

import importlib
import inspect
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from types import ModuleType

from ..types import JSONData
from . import xml
from .enums import FileFormat
from .enums import infer_file_format_and_compression

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'File',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _accepts_root_tag(handler: object) -> bool:
    """
    Return True when *handler* supports a ``root_tag`` argument.

    Parameters
    ----------
    handler : object
        Callable to inspect.

    Returns
    -------
    bool
        True if ``root_tag`` is accepted by the handler.
    """
    if not callable(handler):
        return False
    try:
        signature = inspect.signature(handler)
    except (TypeError, ValueError):
        return False
    for param in signature.parameters.values():
        if param.kind is param.VAR_KEYWORD:
            return True
    return 'root_tag' in signature.parameters


@cache
def _module_for_format(file_format: FileFormat) -> ModuleType:
    """
    Import and return the module for *file_format*.

    Parameters
    ----------
    file_format : FileFormat
        File format enum value.

    Returns
    -------
    ModuleType
        The module implementing IO for the format.
    """
    return importlib.import_module(f'{__package__}.{file_format.value}')


# SECTION: CLASSES ========================================================== #


@dataclass(slots=True)
class File:
    """
    Convenience wrapper around structured file IO.

    This class encapsulates the one-off helpers in this module as convenient
    instance methods while retaining the original function API for
    backward compatibility (those functions delegate to this class).

    Attributes
    ----------
    path : Path
        Path to the file on disk.
    file_format : FileFormat | None, optional
        Explicit format. If omitted, the format is inferred from the file
        extension (``.csv``, ``.json``, etc.).

    Parameters
    ----------
    path : StrPath
        Path to the file on disk.
    file_format : FileFormat | str | None, optional
        Explicit format. If omitted, the format is inferred from the file
        extension (``.csv``, ``.json``, etc.).
    """

    # -- Attributes -- #

    path: Path
    file_format: FileFormat | None = None

    # -- Magic Methods (Object Lifecycle) -- #

    def __post_init__(self) -> None:
        """
        Auto-detect and set the file format on initialization.

        If no explicit :attr:`file_format` is provided, attempt to infer it
        from the file path's extension and update :attr:`file_format`. If the
        extension is unknown, the attribute is left as ``None`` and will be
        validated later by :meth:`_ensure_format`.
        """
        self.path = Path(self.path)
        self.file_format = self._coerce_format(self.file_format)
        if self.file_format is None:
            self.file_format = self._maybe_guess_format()

    # -- Internal Instance Methods -- #

    def _assert_exists(self) -> None:
        """
        Raise FileNotFoundError if :attr:`path` does not exist.

        This centralizes existence checks across multiple read methods.
        """
        if not self.path.exists():
            raise FileNotFoundError(f'File not found: {self.path}')

    def _coerce_format(
        self,
        file_format: FileFormat | str | None,
    ) -> FileFormat | None:
        """
        Normalize the file format input.

        Parameters
        ----------
        file_format : FileFormat | str | None
            File format specifier. Strings are coerced into
            :class:`FileFormat`.

        Returns
        -------
        FileFormat | None
            A normalized file format, or ``None`` when unspecified.
        """
        if file_format is None or isinstance(file_format, FileFormat):
            return file_format
        return FileFormat.coerce(file_format)

    def _ensure_format(self) -> FileFormat:
        """
        Resolve the active format, guessing from extension if needed.

        Returns
        -------
        FileFormat
            The resolved file format.
        """
        return (
            self.file_format
            if self.file_format is not None
            else self._guess_format()
        )

    def _guess_format(self) -> FileFormat:
        """
        Infer the file format from the filename extension.

        Returns
        -------
        FileFormat
            The inferred file format based on the file extension.

        Raises
        ------
        ValueError
            If the extension is unknown or unsupported.
        """
        fmt, compression = infer_file_format_and_compression(self.path)
        if fmt is not None:
            return fmt
        if compression is not None:
            raise ValueError(
                'Cannot infer file format from compressed file '
                f'{self.path!r} with compression {compression.value!r}',
            )
        raise ValueError(
            f'Cannot infer file format from extension {self.path.suffix!r}',
        )

    def _maybe_guess_format(self) -> FileFormat | None:
        """
        Try to infer the format, returning ``None`` if it cannot be inferred.

        Returns
        -------
        FileFormat | None
            The inferred format, or ``None`` if inference fails.
        """
        try:
            return self._guess_format()
        except ValueError:
            # Leave as None; _ensure_format() will raise on use if needed.
            return None

    def _resolve_handler(self, name: str) -> object:
        """
        Resolve a handler from the module for the active file format.

        Parameters
        ----------
        name : str
            Attribute name to resolve (``'read'`` or ``'write'``).

        Returns
        -------
        object
            Callable handler exported by the module.

        Raises
        ------
        ValueError
            If the resolved file format is unsupported.
        """
        module = self._resolve_module()
        try:
            return getattr(module, name)
        except AttributeError as e:
            raise ValueError(
                f'Module {module.__name__} does not implement {name}()',
            ) from e

    def _resolve_module(self) -> ModuleType:
        """
        Resolve the IO module for the active file format.

        Returns
        -------
        ModuleType
            The module that implements read/write for the format.

        Raises
        ------
        ValueError
            If the resolved file format is unsupported.
        """
        fmt = self._ensure_format()
        try:
            return _module_for_format(fmt)
        except ModuleNotFoundError as e:
            raise ValueError(f'Unsupported format: {fmt}') from e

    # -- Instance Methods -- #

    def read(self) -> JSONData:
        """
        Read structured data from :attr:path` using :attr:`file_format`.

        Returns
        -------
        JSONData
            The structured data read from the file.

        Raises
        ------
        TypeError
            If the resolved 'read' handler is not callable.
        """
        self._assert_exists()
        reader = self._resolve_handler('read')
        if callable(reader):
            return reader(self.path)
        else:
            raise TypeError(
                f"'read' handler for format {self.file_format} "
                'is not callable',
            )

    def write(
        self,
        data: JSONData,
        *,
        root_tag: str = xml.DEFAULT_XML_ROOT,
    ) -> int:
        """
        Write *data* to *path* using :attr:`file_format`.

        Parameters
        ----------
        data : JSONData
            Data to write to the file.
        root_tag : str, optional
            Root tag name to use when writing XML files. Defaults to
            ``xml.DEFAULT_XML_ROOT``.

        Returns
        -------
        int
            The number of records written.

        Raises
        ------
        TypeError
            If the resolved 'write' handler is not callable.
        """
        writer = self._resolve_handler('write')
        if not callable(writer):
            raise TypeError(
                f"'write' handler for format {self.file_format} "
                'is not callable',
            )
        if _accepts_root_tag(writer):
            return writer(self.path, data, root_tag=root_tag)
        return writer(self.path, data)
