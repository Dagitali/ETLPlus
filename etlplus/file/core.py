"""
:mod:`etlplus.file.core` module.

Shared helpers for reading and writing structured and semi-structured data
files.
"""

from __future__ import annotations

import shutil
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from dataclasses import field
from dataclasses import replace
from pathlib import Path
from pathlib import PurePath
from typing import Any
from typing import cast

from ..storage import StorageLocation
from ..storage import get_backend
from ..utils.types import StrPath
from . import xml
from .base import BoundFileHandler
from .base import FileHandlerABC
from .base import ReadOptions
from .base import WriteOptions
from .enums import FileFormat
from .enums import infer_file_format_and_compression
from .registry import get_handler

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'File',
]


# SECTION: TYPE ALIASES ===================================================== #


# File formats can be:
# 1. Enums
# 2. Strings coercible to enums
# 3. Left as None for inference.
type FileFormatArg = FileFormat | str | None

# Remote storage URIs are accepted as the ``str`` arm of ``StrPath``.
type FilePathArg = StrPath


# SECTION: CLASSES ========================================================== #


@dataclass(init=False, slots=True)
class File:
    """
    Convenience wrapper around structured file IO.

    This class encapsulates the one-off helpers in this module as convenient
    instance methods while retaining the original function API for
    backward compatibility (those functions delegate to this class).

    Attributes
    ----------
    file_format : FileFormat | None, optional
        Explicit format. If omitted, the format is inferred from the file
        extension (``.csv``, ``.json``, etc.).
    location : StorageLocation
        Parsed storage location.

    Parameters
    ----------
    path : FilePathArg
        Local filesystem path supplied as ``str``/``Path``/``PathLike[str]``,
        or a remote storage URI supplied as ``str`` such as
        ``s3://bucket/file.csv``, ``https://example.com/files/data.csv``, or
        ``https://account.blob.core.windows.net/container/file.csv``.
    file_format : FileFormat | str | None, optional
        Explicit format. If omitted, the format is inferred from the file
        extension (``.csv``, ``.json``, etc.).
    """

    # -- Instance Attributes -- #

    file_format: FileFormat | None = None
    location: StorageLocation = field(init=False, repr=False)

    # -- Magic Methods (Object Lifecycle) -- #

    def __init__(
        self,
        path: FilePathArg,
        file_format: FileFormat | str | None = None,
    ) -> None:
        self.location = StorageLocation.from_value(path)
        self.file_format = self._coerce_format(file_format)
        if self.file_format is None:
            self.file_format = self._maybe_guess_format()

    # -- Magic Methods (Object Representation) -- #

    def __repr__(self) -> str:
        """Return a concise debug representation preserving the public path."""
        return (
            f'{self.__class__.__name__}('
            f'path={self.path!r}, file_format={self.file_format!r})'
        )

    # -- Getters -- #

    @property
    def path(self) -> FilePathArg:
        """Return the public path view derived from :attr:`location`."""
        if self.location.is_local:
            return self.location.as_path()
        return self.location.raw

    # -- Internal Instance Methods -- #

    def _assert_exists(self) -> None:
        """
        Raise FileNotFoundError if :attr:`path` does not exist.

        This centralizes existence checks across multiple read methods.
        """
        if self.location.is_local:
            if not self.location.as_path().exists():
                raise FileNotFoundError(f'File not found: {self.path}')
            return

        if not get_backend(self.location).exists(self.location):
            raise FileNotFoundError(f'File not found: {self.location.raw}')

    def _bound_handler(
        self,
        path: Path,
        *,
        handler: FileHandlerABC | None = None,
    ) -> BoundFileHandler:
        """
        Resolve and bind the active handler to *path*.

        Parameters
        ----------
        path : Path
            Local file path to bind to the resolved handler.
        handler : FileHandlerABC | None, optional
            Explicit handler instance to bind. When omitted, the handler is
            resolved from the active file format.

        Returns
        -------
        BoundFileHandler
            A handler instance bound to *path* for the active format.
        """
        return (handler or self._resolve_handler()).at(path)

    def _coerce_format(
        self,
        file_format: FileFormatArg,
    ) -> FileFormat | None:
        """
        Normalize the file format input.

        Parameters
        ----------
        file_format : FileFormatArg
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

    @contextmanager
    def _dispatch_path(
        self,
        *,
        for_write: bool,
    ) -> Iterator[Path]:
        """
        Yield one local path for handler dispatch.

        Local files are dispatched directly. Remote objects are staged through
        a temporary local path so the existing path-based file handlers can
        run unchanged.
        """
        if self.location.is_local:
            yield self.location.as_path()
            return

        backend = get_backend(self.location)
        filename = self._staging_filename()
        with tempfile.TemporaryDirectory() as tmpdir:
            dispatch_path = Path(tmpdir) / filename
            if not for_write:
                with backend.open(self.location, 'rb') as source:
                    with dispatch_path.open('wb') as target:
                        shutil.copyfileobj(source, target)
            yield dispatch_path
            if for_write:
                backend.ensure_parent_dir(self.location)
                with dispatch_path.open('rb') as source:
                    with backend.open(self.location, 'wb') as target:
                        shutil.copyfileobj(source, target)

    def _ensure_format(self) -> FileFormat:
        """
        Resolve the active format, guessing from extension if needed.

        Returns
        -------
        FileFormat
            The resolved file format.
        """
        return (
            self.file_format if self.file_format is not None else self._guess_format()
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
        suffix_source: object = (
            self.location.as_path() if self.location.is_local else self.location.path
        )
        fmt, compression = infer_file_format_and_compression(suffix_source)
        if fmt is not None:
            return fmt
        if compression is not None:
            raise ValueError(
                'Cannot infer file format from compressed file '
                f'{self.location.raw!r} with compression {compression.value!r}',
            )
        raise ValueError(
            'Cannot infer file format from extension '
            f'{PurePath(str(suffix_source)).suffix!r}',
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

    def _resolve_handler(self) -> FileHandlerABC:
        """
        Resolve a class-based file handler for the active format.

        Returns
        -------
        FileHandlerABC
            Handler instance for the active file format.
        """
        fmt = self._ensure_format()
        return get_handler(fmt)

    def _resolved_write_options(
        self,
        *,
        options: WriteOptions | None,
        root_tag: str,
    ) -> WriteOptions:
        """Return write options with one resolved XML root tag."""
        if options is None:
            return WriteOptions(root_tag=root_tag)
        if root_tag == xml.DEFAULT_XML_ROOT or options.root_tag == root_tag:
            return options
        return replace(options, root_tag=root_tag)

    def _staging_filename(self) -> str:
        """Return one safe staging filename for remote dispatch."""
        filename = PurePath(self.location.path).name
        if filename:
            return filename
        if self.file_format is not None:
            return f'payload.{self.file_format.value}'
        return 'payload.tmp'

    # -- Instance Methods -- #

    def delete(self) -> None:
        """Delete :attr:`path` through the active storage backend."""
        get_backend(self.location).delete(self.location)

    def exists(self) -> bool:
        """Return whether :attr:`path` currently exists."""
        if self.location.is_local:
            return self.location.as_path().exists()
        return get_backend(self.location).exists(self.location)

    def read(
        self,
        *,
        options: ReadOptions | None = None,
        handler: FileHandlerABC | None = None,
    ) -> Any:
        """
        Read structured data from :attr:`path` using :attr:`file_format`.

        Parameters
        ----------
        options : ReadOptions | None, optional
            Optional read parameters forwarded to the active handler.
        handler : FileHandlerABC | None, optional
            Explicit handler instance to use instead of resolving one from the
            registry. This is primarily used by bound handler facades so they
            preserve handler-specific behavior for remote URIs.

        Returns
        -------
        Any
            The parsed data read from the file.
        """
        self._assert_exists()
        with self._dispatch_path(for_write=False) as path:
            bound_handler = self._bound_handler(path, handler=handler)
            if options is None:
                return bound_handler.read()
            return bound_handler.read(options=options)

    def write(
        self,
        data: object,
        *,
        options: WriteOptions | None = None,
        root_tag: str = xml.DEFAULT_XML_ROOT,
        handler: FileHandlerABC | None = None,
    ) -> int:
        """
        Write *data* to *path* using :attr:`file_format`.

        Parameters
        ----------
        data : object
            Data to write to the file.
        options : WriteOptions | None, optional
            Optional write parameters forwarded to the active handler.
        root_tag : str, optional
            Root tag name to use when writing XML files. Defaults to
            ``xml.DEFAULT_XML_ROOT``.
        handler : FileHandlerABC | None, optional
            Explicit handler instance to use instead of resolving one from the
            registry. This is primarily used by bound handler facades so they
            preserve handler-specific behavior for remote URIs.

        Returns
        -------
        int
            The number of records written.

        """
        resolved_options = self._resolved_write_options(
            options=options,
            root_tag=root_tag,
        )
        with self._dispatch_path(for_write=True) as path:
            return cast(Any, self._bound_handler(path, handler=handler)).write(
                data,
                options=resolved_options,
            )
