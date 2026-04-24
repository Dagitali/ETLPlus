"""
:mod:`etlplus.ops._imports` module.

Internal dependency import helpers for :mod:`ops` modules.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

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


_MODULE_CACHE: dict[str, Any] = {}


# SECTION: INTERNAL FUNCTIONS =============================================== #


def get_dependency(
    module_name: str,
    *,
    error_message: str,
) -> Any:
    """Import one validation dependency with a runtime-oriented error."""
    return import_package(
        module_name,
        error_message=error_message,
        cache=_MODULE_CACHE,
        importer=import_module,
        error_type=RuntimeError,
        import_exceptions=Exception,
    )


# SECTION: FUNCTIONS ======================================================== #


def get_frictionless() -> Any:
    """Import and return :mod:`frictionless` lazily."""
    return get_dependency(
        'frictionless',
        error_message=(
            'frictionless is required for CSV schema validation. '
            'Install with: pip install frictionless'
        ),
    )


def get_jsonschema() -> Any:
    """Import and return :mod:`jsonschema` lazily."""
    return get_dependency(
        'jsonschema',
        error_message=(
            'jsonschema is required for JSON Schema validation. '
            'Install with: pip install jsonschema'
        ),
    )


def get_lxml_etree() -> Any:
    """Import and return :mod:`lxml.etree` lazily."""
    return get_dependency(
        'lxml.etree',
        error_message=(
            'lxml is required for XML schema validation. '
            'Install with: pip install lxml'
        ),
    )


def get_yaml() -> Any:
    """Import and return :mod:`yaml` lazily."""
    return get_dependency(
        'yaml',
        error_message=(
            'PyYAML is required for YAML schema validation. '
            'Install with: pip install PyYAML'
        ),
    )
