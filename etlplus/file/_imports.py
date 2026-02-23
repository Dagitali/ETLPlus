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
