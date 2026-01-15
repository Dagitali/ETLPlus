"""
:mod:`etlplus.file._imports` module.

Shared helpers for optional dependency imports.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

# SECTION: INTERNAL CONSTANTS =============================================== #


_MODULE_CACHE: dict[str, Any] = {}


# SECTION: FUNCTIONS ======================================================== #


def get_optional_module(module_name: str, *, error_message: str) -> Any:
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
