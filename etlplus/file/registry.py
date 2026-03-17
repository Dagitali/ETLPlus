"""
:mod:`etlplus.file.registry` module.

Class-based file handler registry.
"""

from __future__ import annotations

import importlib
from functools import cache
from typing import cast

from .base import FileHandlerABC
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'get_handler',
    'get_handler_class',
]


# SECTION: TYPE ALIASES ===================================================== #


type HandlerClass = type[FileHandlerABC]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _coerce_handler_class(
    symbol: object,
    *,
    file_format: FileFormat,
) -> HandlerClass:
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
    HandlerClass
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
            f'Handler for {file_format.value!r} must inherit FileHandlerABC',
        )
    handler_class = cast(HandlerClass, symbol)
    declared_format = getattr(handler_class, 'format', None)
    if declared_format is not file_format:
        raise ValueError(
            f'Handler for {file_format.value!r} declares mismatched format '
            f'{declared_format!r}',
        )
    return handler_class


def _handler_class_name(
    file_format: FileFormat,
) -> str:
    """Build the canonical handler class name for one format."""
    value = file_format.value
    return f'{value[:1].upper()}{value[1:]}File'


def _handler_spec(
    file_format: FileFormat,
) -> str:
    """Build the ``module:Class`` import spec for one handler format."""
    return f'etlplus.file.{file_format.value}:{_handler_class_name(file_format)}'


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
    module_name, separator, attr = spec.partition(':')
    if separator != ':' or not module_name or not attr:
        raise ValueError(f'Invalid handler spec: {spec!r}')
    module = importlib.import_module(module_name)
    try:
        return getattr(module, attr)
    except AttributeError as e:
        raise ValueError(
            f'Handler symbol {attr!r} not found in module {module_name!r}',
        ) from e


# SECTION: INTERNAL CONSTANTS =============================================== #


_SUPPORTED_HANDLER_FORMATS: tuple[FileFormat, ...] = (
    # Stubbed / placeholder
    FileFormat.STUB,
    FileFormat.ACCDB,
    FileFormat.CFG,
    FileFormat.CONF,
    FileFormat.ION,
    FileFormat.MDB,
    FileFormat.NUMBERS,
    FileFormat.PBF,
    FileFormat.WKS,
    # Tabular / delimited text
    FileFormat.CSV,
    FileFormat.DAT,
    FileFormat.FWF,
    FileFormat.PSV,
    FileFormat.TAB,
    FileFormat.TSV,
    FileFormat.TXT,
    # Semi-structured text
    FileFormat.INI,
    FileFormat.JSON,
    FileFormat.NDJSON,
    FileFormat.PROPERTIES,
    FileFormat.TOML,
    FileFormat.XML,
    FileFormat.YAML,
    # Columnar / analytics
    FileFormat.ARROW,
    FileFormat.FEATHER,
    FileFormat.ORC,
    FileFormat.PARQUET,
    # Binary serialization
    FileFormat.AVRO,
    FileFormat.BSON,
    FileFormat.CBOR,
    FileFormat.MSGPACK,
    FileFormat.PB,
    FileFormat.PROTO,
    # Databases / embedded storage
    FileFormat.DUCKDB,
    FileFormat.SQLITE,
    # Spreadsheet
    FileFormat.ODS,
    FileFormat.XLS,
    FileFormat.XLSM,
    FileFormat.XLSX,
    # Scientific / statistical
    FileFormat.DTA,
    FileFormat.HDF5,
    FileFormat.MAT,
    FileFormat.NC,
    FileFormat.RDA,
    FileFormat.RDS,
    FileFormat.SAS7BDAT,
    FileFormat.SAV,
    FileFormat.SYLK,
    FileFormat.XPT,
    FileFormat.ZSAV,
    # Archives / wrappers
    FileFormat.GZ,
    FileFormat.ZIP,
    # Logs
    FileFormat.LOG,
    # Templates
    FileFormat.HBS,
    FileFormat.JINJA2,
    FileFormat.MUSTACHE,
    FileFormat.VM,
)


# Explicit class map is the only dispatch path.
_HANDLER_CLASS_SPECS: dict[FileFormat, str] = {
    file_format: _handler_spec(file_format)
    for file_format in _SUPPORTED_HANDLER_FORMATS
}


# SECTION: FUNCTIONS ======================================================== #


@cache
def get_handler_class(
    file_format: FileFormat,
) -> HandlerClass:
    """
    Resolve a handler class for *file_format*.

    Parameters
    ----------
    file_format : FileFormat
        File format enum value.

    Returns
    -------
    HandlerClass
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
        except (ModuleNotFoundError, ValueError) as e:
            raise ValueError(f'Unsupported format: {file_format}') from e
        return _coerce_handler_class(symbol, file_format=file_format)

    raise ValueError(f'Unsupported format: {file_format}')


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
