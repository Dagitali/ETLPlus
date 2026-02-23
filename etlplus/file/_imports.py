"""
:mod:`etlplus.file._imports` module.

Shared helpers for optional dependency imports.
"""

from __future__ import annotations

import sys
from collections.abc import Callable
from importlib import import_module
from typing import Any

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'get_dependency',
    'get_optional_module',
    'get_pandas',
    'get_pyarrow',
    'get_yaml',
    'resolve_dependency',
    'resolve_module_callable',
    'resolve_pandas',
]


# SECTION: INTERNAL CONSTANTS =============================================== #


# Optional Python module support (lazy-loaded to avoid hard dependency)
_MODULE_CACHE: dict[str, Any] = {}


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _error_message(
    module_name: str,
    format_name: str,
    pip_name: str | None = None,
) -> str:
    """
    Build an import error message for an optional dependency.

    Parameters
    ----------
    module_name : str
        Module name to look up.
    format_name : str
        Human-readable format name for templated messages.
    pip_name : str | None, optional
        Package name to suggest for installation. Defaults to *module_name*.

    Returns
    -------
    str
        Formatted error message.
    """
    install_name = pip_name or module_name
    return (
        f'{format_name} support requires '
        f'optional dependency "{install_name}".\n'
        f'Install with: pip install {install_name}'
    )


def _resolve_with_module_override(
    handler: object,
    override_name: str,
    fallback: Callable[..., Any],
    /,
    *args: Any,
    **kwargs: Any,
) -> Any:
    """
    Resolve one dependency call via module override with fallback.

    Parameters
    ----------
    handler : object
        The handler instance whose concrete module may override resolution.
    override_name : str
        Callable name to resolve from the concrete module.
    fallback : Callable[..., Any]
        Fallback resolver when no override callable is present.
    *args : Any
        Positional arguments forwarded to the resolver.
    **kwargs : Any
        Keyword arguments forwarded to the resolver.

    Returns
    -------
    Any
        The resolved dependency/module value.
    """
    if resolver := resolve_module_callable(handler, override_name):
        return resolver(*args, **kwargs)
    return fallback(*args, **kwargs)


# SECTION: FUNCTIONS ======================================================== #


def get_optional_module(
    module_name: str,
    *,
    error_message: str,
) -> Any:
    """
    Return an optional dependency module, caching on first import.

    Parameters
    ----------
    module_name : str
        Name of the module to import.
    error_message : str
        Error message to surface when the module is missing.

    Returns
    -------
    Any
        The imported module.

    Raises
    ------
    ImportError
        If the optional dependency is missing.
    """
    try:
        return _MODULE_CACHE[module_name]
    except KeyError:
        pass
    try:
        module = import_module(module_name)
    except ImportError as e:  # pragma: no cover
        missing_name = getattr(e, 'name', None)
        if missing_name is not None and missing_name != module_name:
            raise
        raise ImportError(error_message) from e
    _MODULE_CACHE[module_name] = module
    return module


def get_dependency(
    module_name: str,
    *,
    format_name: str,
    pip_name: str | None = None,
) -> Any:
    """
    Return an optional dependency module with a standardized error message.

    Parameters
    ----------
    module_name : str
        Name of the module to import.
    format_name : str
        Human-readable format name for error messages.
    pip_name : str | None, optional
        Package name to suggest for installation (defaults to *module_name*).

    Returns
    -------
    Any
        The imported module.
    """
    return get_optional_module(
        module_name,
        error_message=_error_message(
            module_name,
            format_name=format_name,
            pip_name=pip_name,
        ),
    )


def get_pandas(
    format_name: str,
) -> Any:
    """
    Return the pandas module, importing it on first use.

    Parameters
    ----------
    format_name : str
        Human-readable format name for error messages.

    Returns
    -------
    Any
        The pandas module.
    """
    return get_dependency('pandas', format_name=format_name)


def get_pyarrow(
    format_name: str,
) -> Any:
    """
    Return the pyarrow module, importing it on first use.

    Parameters
    ----------
    format_name : str
        Human-readable format name for error messages.

    Returns
    -------
    Any
        The pyarrow module.
    """
    return get_dependency('pyarrow', format_name=format_name)


def get_yaml() -> Any:
    """
    Return the PyYAML module, importing it on first use.

    Returns
    -------
    Any
        The PyYAML module.
    """
    return get_dependency('yaml', format_name='YAML', pip_name='PyYAML')


def resolve_module_callable(
    handler: object,
    name: str,
) -> Callable[..., Any] | None:
    """
    Resolve one callable from the concrete handler module when present.

    Parameters
    ----------
    handler : object
        The handler instance whose concrete module should be inspected.
    name : str
        The callable name to resolve from the concrete module.

    Returns
    -------
    Callable[..., Any] | None
        The resolved callable if present and callable; otherwise ``None``.
    """
    module = sys.modules.get(type(handler).__module__)
    if module is None:
        return None
    value = getattr(module, name, None)
    return value if callable(value) else None


def resolve_dependency(
    handler: object,
    dependency_name: str,
    *,
    format_name: str,
    pip_name: str | None = None,
) -> Any:
    """
    Resolve one optional dependency with module-level override support.

    Parameters
    ----------
    handler : object
        The handler instance whose concrete module may override resolution.
    dependency_name : str
        Dependency import name.
    format_name : str
        Human-readable format name used for import error context.
    pip_name : str | None, optional
        Optional package name hint.

    Returns
    -------
    Any
        The resolved dependency module.
    """
    return _resolve_with_module_override(
        handler,
        'get_dependency',
        get_dependency,
        dependency_name,
        format_name=format_name,
        pip_name=pip_name,
    )


def resolve_pandas(
    handler: object,
    *,
    format_name: str,
) -> Any:
    """
    Resolve pandas with module-level override support.

    Parameters
    ----------
    handler : object
        The handler instance whose concrete module may override resolution.
    format_name : str
        Human-readable format name used for import error context.

    Returns
    -------
    Any
        The resolved pandas module.
    """
    return _resolve_with_module_override(
        handler,
        'get_pandas',
        get_pandas,
        format_name,
    )
