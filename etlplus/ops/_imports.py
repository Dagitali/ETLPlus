"""
:mod:`etlplus.ops._imports` module.

Internal dependency import helpers for :mod:`ops` modules.
"""

from __future__ import annotations

from typing import Any

from ..utils._imports import DependencyImporter

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
_DEPENDENCY_IMPORTER = DependencyImporter(
    error_type=RuntimeError,
    import_exceptions=Exception,
)


# SECTION: FUNCTIONS ======================================================== #


def get_dependency(
    module_name: str,
    *,
    format_name: str,
    pip_name: str | None = None,
    required: bool = False,
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
    return _DEPENDENCY_IMPORTER.get(
        module_name,
        format_name=format_name,
        pip_name=pip_name,
        required=required,
    )


def get_frictionless() -> Any:
    """
    Return :mod:`frictionless` lazily (i.e, importing it on first use).

    Returns
    -------
    Any
        The :mod:`frictionless` module.
    """
    return get_dependency(
        'frictionless',
        format_name='CSV schema',
        required=True,
    )


def get_jsonschema() -> Any:
    """
    Return :mod:`jsonschema` lazily (i.e, importing it on first use).

    Returns
    -------
    Any
        The :mod:`jsonschema` module.
    """
    return get_dependency(
        'jsonschema',
        format_name='JSON Schema',
        required=True,
    )


def get_lxml_etree() -> Any:
    """
    Return :mod:`lxml.etree` lazily (i.e, importing it on first use).

    Returns
    -------
    Any
        The :mod:`lxml.etree` module.
    """
    return get_dependency(
        'lxml.etree',
        format_name='XML schema',
        pip_name='lxml',
        required=True,
    )


def get_yaml() -> Any:
    """
    Return :mod:`yaml` lazily (i.e, importing it on first use)

    Returns
    -------
    Any
        The :mod:`yaml` module.
    """
    return get_dependency(
        'yaml',
        format_name='YAML schema',
        pip_name='PyYAML',
        required=True,
    )
