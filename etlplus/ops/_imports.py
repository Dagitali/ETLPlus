"""
:mod:`etlplus.ops._imports` module.

Internal dependency import helpers for :mod:`ops` modules.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

from ..utils._imports import build_dependency_error_message
from ..utils._imports import import_package

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'get_dependency',
    'get_frictionless',
    'get_jsonschema',
    'get_lxml_etree',
    'get_yaml',
]


# SECTION: INTERNAL CONSTANTS =============================================== #


# Dependency module support (lazy-loaded to avoid hard dependency)
_MODULE_CACHE: dict[str, Any] = {}


# SECTION: FUNCTIONS ======================================================== #


def get_dependency(
    module_name: str,
    *,
    format_name: str,
    pip_name: str | None = None,
    required: bool = True,
) -> Any:
    """
    Return a dependency module with a standardized runtime error message.

    Parameters
    ----------
    module_name : str
        Name of the module to import.
    format_name : str
        Human-readable format name for error messages.
    pip_name : str | None, optional
        Package name to suggest for installation (defaults to *module_name*).
    required : bool, optional
        Whether to use required-dependency message wording.
        Defaults to ``False`` (optional dependency wording).

    Returns
    -------
    Any
        The imported module.
    """
    error_message = build_dependency_error_message(
        module_name,
        format_name=format_name,
        pip_name=pip_name,
        required=required,
    )
    return import_package(
        module_name,
        error_message=error_message,
        cache=_MODULE_CACHE,
        importer=import_module,
        error_type=RuntimeError,
        import_exceptions=Exception,
    )


def get_frictionless() -> Any:
    """
    Import and return :mod:`frictionless` lazily.

    Returns
    -------
    Any
        The :mod:`frictionless` module.
    """
    return get_dependency(
        'frictionless',
        format_name='CSV schema',
    )


def get_jsonschema() -> Any:
    """
    Import and return :mod:`jsonschema` lazily.

    Returns
    -------
    Any
        The :mod:`jsonschema` module.
    """
    return get_dependency(
        'jsonschema',
        format_name='JSON Schema',
    )


def get_lxml_etree() -> Any:
    """
    Import and return :mod:`lxml.etree` lazily.

    Returns
    -------
    Any
        The :mod:`lxml.etree` module.
    """
    return get_dependency(
        'lxml.etree',
        format_name='XML schema',
        pip_name='lxml',
    )


def get_yaml() -> Any:
    """
    Import and return :mod:`yaml` lazily.

    Returns
    -------
    Any
        The :mod:`yaml` module.
    """
    return get_dependency(
        'yaml',
        format_name='YAML schema',
        pip_name='PyYAML',
    )
