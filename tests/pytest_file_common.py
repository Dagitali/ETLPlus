"""
:mod:`tests.pytest_file_common` module.

Shared helpers for file-focused tests.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from types import ModuleType
from typing import Literal
from typing import cast
from typing import overload

from etlplus.file import FileFormat
from etlplus.file.base import FileHandlerABC
from etlplus.file.base import WriteOptions
from etlplus.types import JSONData

# SECTION: CONSTANTS ======================================================== #


ORC_SYSCTL_SKIP_REASON = 'ORC read failed due to sysctl limitations'


# SECTION: TYPE ALIASES ===================================================== #


type Operation = Literal['read', 'write']


# SECTION: FUNCTIONS ======================================================== #


def _resolve_write_options(
    write_kwargs: Mapping[str, object] | None = None,
) -> WriteOptions | None:
    """
    Normalize optional write kwargs into a ``WriteOptions`` instance.
    """
    if write_kwargs is None:
        return None

    kwargs = dict(write_kwargs)
    root_tag = kwargs.pop('root_tag', None)
    options = kwargs.pop('options', None)

    if kwargs:
        invalid = ', '.join(sorted(kwargs))
        raise TypeError(f'unsupported write kwargs: {invalid}')

    if root_tag is not None:
        if not isinstance(root_tag, str):
            raise TypeError('root_tag must be a string')
        if options is None:
            options = WriteOptions(root_tag=root_tag)

    if options is None:
        return None
    if not isinstance(options, WriteOptions):
        raise TypeError('options must be a WriteOptions instance')
    return options


@overload
def call_handler_operation(
    module: ModuleType,
    *,
    operation: Literal['read'],
    path: Path,
    payload: None = None,
    write_kwargs: Mapping[str, object] | None = None,
) -> JSONData: ...


@overload
def call_handler_operation(
    module: ModuleType,
    *,
    operation: Literal['write'],
    path: Path,
    payload: JSONData,
    write_kwargs: Mapping[str, object] | None = None,
) -> int: ...


def call_handler_operation(
    module: ModuleType,
    *,
    operation: Operation,
    path: Path,
    payload: JSONData | None = None,
    write_kwargs: Mapping[str, object] | None = None,
) -> JSONData | int:
    """Call one module handler operation with normalized write options."""
    handler = resolve_module_handler(module)
    if operation == 'read':
        return handler.read(path)
    if payload is None:
        raise TypeError('payload is required for write operations')
    options = _resolve_write_options(write_kwargs)
    return handler.write(path, payload, options=options)


def is_orc_sysctl_error(error: OSError) -> bool:
    """Return whether one ORC I/O failure matches known sysctl constraints."""
    return 'sysctlbyname' in str(error)


def resolve_module_handler(
    module: ModuleType,
) -> FileHandlerABC:
    """Return the singleton handler instance defined by a file module."""
    handlers = [
        value
        for name, value in vars(module).items()
        if name.endswith('_HANDLER')
    ]
    assert len(handlers) == 1
    handler = handlers[0]
    assert isinstance(handler, FileHandlerABC)
    return cast(FileHandlerABC, handler)


def should_skip_known_file_io_error(
    *,
    error: OSError,
    file_format: FileFormat | None = None,
    module_name: str | None = None,
) -> bool:
    """Return whether one file I/O error should be skipped in tests."""
    is_orc_failure = (
        file_format is FileFormat.ORC
        or (
            module_name is not None
            and module_name.endswith('.orc')
        )
    )
    return is_orc_failure and is_orc_sysctl_error(error)
