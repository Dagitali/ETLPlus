"""
:mod:`etlplus.ops._imports` module.

Internal dependency import helpers for :mod:`ops` modules.
"""

from __future__ import annotations

from typing import Any

from ..utils._imports import import_package

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'import_frictionless',
    'import_jsonschema',
    'import_lxml_etree',
    'import_yaml',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def import_validation_dependency(
    module_name: str,
    *,
    error_message: str,
) -> Any:
    """Import one validation dependency with a runtime-oriented error."""
    return import_package(
        module_name,
        error_message=error_message,
        error_type=RuntimeError,
        import_exceptions=Exception,
    )


# SECTION: FUNCTIONS ======================================================== #


def import_frictionless() -> Any:
    """Import and return :mod:`frictionless` lazily."""
    return import_validation_dependency(
        'frictionless',
        error_message=(
            'frictionless is required for CSV schema validation. '
            'Install with: pip install frictionless'
        ),
    )


def import_jsonschema() -> Any:
    """Import and return :mod:`jsonschema` lazily."""
    return import_validation_dependency(
        'jsonschema',
        error_message=(
            'jsonschema is required for JSON Schema validation. '
            'Install with: pip install jsonschema'
        ),
    )


def import_lxml_etree() -> Any:
    """Import and return :mod:`lxml.etree` lazily."""
    return import_validation_dependency(
        'lxml.etree',
        error_message=(
            'lxml is required for XML schema validation. '
            'Install with: pip install lxml'
        ),
    )


def import_yaml() -> Any:
    """Import and return :mod:`yaml` lazily."""
    return import_validation_dependency(
        'yaml',
        error_message=(
            'PyYAML is required for YAML schema validation. '
            'Install with: pip install PyYAML'
        ),
    )
