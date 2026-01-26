"""
:mod:`etlplus.file._imports` module.

Shared helpers for optional dependency imports.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

# SECTION: INTERNAL CONSTANTS =============================================== #


# Optional Python module support (lazy-loaded to avoid hard dependency)
_MODULE_CACHE: dict[str, Any] = {}


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _error_message(
    module_name: str,
    format_name: str,
) -> str:
    """
    Build an import error message for an optional dependency.

    Parameters
    ----------
    module_name : str
        Module name to look up.
    format_name : str
        Human-readable format name for templated messages.

    Returns
    -------
    str
        Formatted error message.
    """
    return (
        f'{format_name} support requires '
        f'optional dependency "{module_name}".\n'
        f'Install with: pip install {module_name}'
    )


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
    cached = _MODULE_CACHE.get(module_name)
    if cached is not None:  # pragma: no cover - tiny branch
        return cached
    try:
        module = import_module(module_name)
    except ImportError as e:  # pragma: no cover
        raise ImportError(error_message) from e
    _MODULE_CACHE[module_name] = module
    return module


def get_fastavro() -> Any:
    """
    Return the fastavro module, importing it on first use.

    Raises an informative ImportError if the optional dependency is missing.

    Notes
    -----
    Prefer :func:`get_optional_module` for new call sites.
    """
    return get_optional_module(
        'fastavro',
        error_message=_error_message('fastavro', format_name='AVRO'),
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

    Notes
    -----
    Prefer :func:`get_optional_module` for new call sites.
    """
    return get_optional_module(
        'pandas',
        error_message=_error_message('pandas', format_name=format_name),
    )


def get_yaml() -> Any:
    """
    Return the PyYAML module, importing it on first use.

    Raises an informative ImportError if the optional dependency is missing.

    Notes
    -----
    Prefer :func:`get_optional_module` for new call sites.
    """
    return get_optional_module(
        'yaml',
        error_message=_error_message('PyYAML', format_name='YAML'),
    )
