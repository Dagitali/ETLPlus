"""
:mod:`etlplus.ops._imports` module.

Internal dependency helpers for ops modules.
"""

from __future__ import annotations

from typing import Any

from ..utils._imports import import_package

# SECTION: EXPORTS ========================================================== #


__all__ = [
    '_import_frictionless',
    '_import_jsonschema',
    '_import_lxml_etree',
    '_import_yaml',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _import_validation_dependency(
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


def _import_frictionless() -> Any:
    """Import and return :mod:`frictionless` lazily."""
    return _import_validation_dependency(
        'frictionless',
        error_message=(
            'frictionless is required for CSV schema validation. '
            'Install with: pip install frictionless'
        ),
    )


def _import_jsonschema() -> Any:
    """Import and return :mod:`jsonschema` lazily."""
    return _import_validation_dependency(
        'jsonschema',
        error_message=(
            'jsonschema is required for JSON Schema validation. '
            'Install with: pip install jsonschema'
        ),
    )


def _import_lxml_etree() -> Any:
    """Import and return :mod:`lxml.etree` lazily."""
    return _import_validation_dependency(
        'lxml.etree',
        error_message=(
            'lxml is required for XML schema validation. '
            'Install with: pip install lxml'
        ),
    )


def _import_yaml() -> Any:
    """Import and return :mod:`yaml` lazily."""
    return _import_validation_dependency(
        'yaml',
        error_message=(
            'PyYAML is required for YAML schema validation. '
            'Install with: pip install PyYAML'
        ),
    )
