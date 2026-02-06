"""
:mod:`etlplus.file.registry` module.

Class-based file handler registry.
"""

from __future__ import annotations

import importlib
import inspect
from collections.abc import Callable
from functools import cache
from pathlib import Path
from types import ModuleType
from typing import cast

from ..types import JSONData
from .base import FileHandlerABC
from .base import ReadOptions
from .base import WriteOptions
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'get_handler',
    'get_handler_class',
]


# SECTION: INTERNAL CONSTANTS =============================================== #


_HANDLER_CLASS_SPECS: dict[FileFormat, str] = {
    FileFormat.CSV: 'etlplus.file.csv:CsvFile',
    FileFormat.DAT: 'etlplus.file.dat:DatFile',
    FileFormat.PSV: 'etlplus.file.psv:PsvFile',
    FileFormat.TAB: 'etlplus.file.tab:TabFile',
    FileFormat.TSV: 'etlplus.file.tsv:TsvFile',
}


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


def _coerce_handler_class(
    symbol: object,
    *,
    file_format: FileFormat,
) -> type[FileHandlerABC]:
    """
    Validate and coerce *symbol* into a handler class.

    Parameters
    ----------
    symbol : object
        Imported object to validate.
    file_format : FileFormat
        File format associated with the imported handler.

    Returns
    -------
    type[FileHandlerABC]
        Concrete handler class.

    Raises
    ------
    ValueError
        If the imported symbol is not a handler class.
    """
    if not isinstance(symbol, type):
        raise ValueError(
            f'Handler for {file_format.value!r} must be a class, got '
            f'{type(symbol).__name__}',
        )
    if not issubclass(symbol, FileHandlerABC):
        raise ValueError(
            f'Handler for {file_format.value!r} must inherit '
            'FileHandlerABC',
        )
    return cast(type[FileHandlerABC], symbol)


def _import_symbol(
    spec: str,
) -> object:
    """
    Import and return a symbol in ``module:attribute`` format.

    Parameters
    ----------
    spec : str
        Import specification in ``module:attribute`` form.

    Returns
    -------
    object
        Imported symbol.

    Raises
    ------
    ValueError
        If *spec* is malformed or symbol import fails.
    """
    parts = spec.split(':', maxsplit=1)
    if len(parts) != 2:
        raise ValueError(f'Invalid handler spec: {spec!r}')
    module_name, attr = parts
    module = importlib.import_module(module_name)
    try:
        return getattr(module, attr)
    except AttributeError as err:
        raise ValueError(
            f'Handler symbol {attr!r} not found in module {module_name!r}',
        ) from err


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


@cache
def _module_adapter_class_for_format(
    file_format: FileFormat,
) -> type[FileHandlerABC]:
    """
    Build a handler class that adapts module-level read/write functions.

    Parameters
    ----------
    file_format : FileFormat
        File format enum value.

    Returns
    -------
    type[FileHandlerABC]
        Handler class wrapping the format module's ``read`` and ``write``
        callables.

    Raises
    ------
    ValueError
        If required functions are missing.
    """
    module = _module_for_format(file_format)
    reader = getattr(module, 'read', None)
    writer = getattr(module, 'write', None)

    if not callable(reader):
        raise ValueError(
            f'Module {module.__name__!r} does not implement callable read()',
        )
    if not callable(writer):
        raise ValueError(
            f'Module {module.__name__!r} does not implement callable write()',
        )

    typed_reader = cast(Callable[[Path], JSONData], reader)
    typed_writer = cast(Callable[..., int], writer)

    class_name = f'{file_format.value.upper()}ModuleHandler'

    class ModuleHandler(FileHandlerABC):
        """Auto-generated handler adapter for "{file_format.value}"."""

        format = file_format
        category = 'module_adapter'

        def read(
            self,
            path: Path,
            *,
            options: ReadOptions | None = None,
        ) -> JSONData:
            _ = options
            return typed_reader(path)

        def write(
            self,
            path: Path,
            data: JSONData,
            *,
            options: WriteOptions | None = None,
        ) -> int:
            if _accepts_root_tag(writer):
                root_tag = (
                    options.root_tag
                    if options is not None
                    else WriteOptions().root_tag
                )
                return typed_writer(path, data, root_tag=root_tag)
            return typed_writer(path, data)

    ModuleHandler.__name__ = class_name
    ModuleHandler.__qualname__ = class_name
    return cast(type[FileHandlerABC], ModuleHandler)


# SECTION: FUNCTIONS ======================================================== #


@cache
def get_handler_class(
    file_format: FileFormat,
) -> type[FileHandlerABC]:
    """
    Resolve a handler class for *file_format*.

    Parameters
    ----------
    file_format : FileFormat
        File format enum value.

    Returns
    -------
    type[FileHandlerABC]
        Concrete handler class for the format.

    Raises
    ------
    ValueError
        If the format has no registered handler.
    """
    spec = _HANDLER_CLASS_SPECS.get(file_format)
    if spec is not None:
        try:
            symbol = _import_symbol(spec)
        except (ModuleNotFoundError, ValueError) as err:
            raise ValueError(f'Unsupported format: {file_format}') from err
        return _coerce_handler_class(symbol, file_format=file_format)

    try:
        return _module_adapter_class_for_format(file_format)
    except (ModuleNotFoundError, ValueError) as err:
        raise ValueError(f'Unsupported format: {file_format}') from err


@cache
def get_handler(
    file_format: FileFormat,
) -> FileHandlerABC:
    """
    Return a singleton handler instance for *file_format*.

    Parameters
    ----------
    file_format : FileFormat
        File format enum value.

    Returns
    -------
    FileHandlerABC
        Singleton handler instance.
    """
    return get_handler_class(file_format)()
