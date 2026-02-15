"""
:mod:`tests.pytest_file_common` module.

Shared helpers for file-focused tests.
"""

from __future__ import annotations

from pathlib import Path
from types import ModuleType
from typing import Any
from typing import Literal
from typing import cast

from etlplus.file.base import FileHandlerABC
from etlplus.file.base import WriteOptions

# SECTION: TYPE ALIASES ===================================================== #


type Operation = Literal['read', 'write']

# SECTION: FUNCTIONS ======================================================== #


def call_handler_operation(
    module: ModuleType,
    *,
    operation: Operation,
    path: Path,
    payload: object | None = None,
    write_kwargs: dict[str, object] | None = None,
) -> object | int:
    """Call one module handler operation with normalized write kwargs."""
    handler = resolve_module_handler(module)
    if operation == 'read':
        return handler.read(path)
    kwargs = normalize_write_kwargs(write_kwargs)
    return cast(Any, handler).write(path, payload, **kwargs)


def normalize_write_kwargs(
    write_kwargs: dict[str, object] | None = None,
) -> dict[str, object]:
    """
    Normalize smoke/unit write kwargs to handler-compatible options.

    Supports converting ``root_tag=...`` into ``options=WriteOptions(...)``.
    """
    kwargs = dict(write_kwargs or {})
    if 'root_tag' in kwargs and 'options' not in kwargs:
        root_tag = kwargs.pop('root_tag')
        if not isinstance(root_tag, str):
            raise TypeError('root_tag must be a string')
        kwargs['options'] = WriteOptions(root_tag=root_tag)
    return kwargs


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
