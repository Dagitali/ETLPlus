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


get_dependency = _DEPENDENCY_IMPORTER.get


def get_frictionless() -> Any:
    """
    Return :mod:`frictionless` lazily, importing on first use.

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
    Return :mod:`jsonschema` lazily, importing on first use.

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
    Return :mod:`lxml.etree` lazily, importing on first use.

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
    Return :mod:`yaml` lazily, importing on first use.

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
