"""
:mod:`tests.pytest_file_common` module.

Shared helpers for file-focused tests.
"""

from __future__ import annotations

from types import ModuleType
from typing import Any

from etlplus.file.base import WriteOptions


def resolve_module_handler(
    module: ModuleType,
) -> Any:
    """Return the singleton handler instance defined by a file module."""
    handlers = [
        value
        for name, value in vars(module).items()
        if name.endswith('_HANDLER')
    ]
    assert len(handlers) == 1
    return handlers[0]


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
