"""
:mod:`etlplus.file._imports` module.

Shared helpers for optional dependency imports.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

# SECTION: INTERNAL CONSTANTS =============================================== #


_MODULE_CACHE: dict[str, Any] = {}

_OPTIONAL_ERRORS: dict[str, str] = {
    'fastavro': (
        'AVRO support requires optional dependency "fastavro".\n'
        'Install with: pip install fastavro'
    ),
    'pandas': (
        '%s support requires optional dependency "pandas".\n'
        'Install with: pip install pandas'
    ),
    'yaml': (
        'YAML support requires optional dependency "PyYAML".\n'
        'Install with: pip install PyYAML'
    ),
}


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _format_error(
    module_name: str,
    *,
    format_name: str | None = None,
) -> str:
    """
    Build an import error message for an optional dependency.

    Parameters
    ----------
    module_name : str
        Module name to look up.
    format_name : str | None, optional
        Optional human-readable format name for templated messages.

    Returns
    -------
    str
        Formatted error message.

    Raises
    ------
    ValueError
        If ``format_name`` is required but not provided.
    """
    template = _OPTIONAL_ERRORS[module_name]
    if '%s' in template:
        if format_name is None:
            raise ValueError(
                f'format_name is required for {module_name} error messages',
            )
        return template % format_name
    return template


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
        error_message=_format_error('fastavro'),
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
        error_message=_format_error('pandas', format_name=format_name),
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
        error_message=_format_error('yaml'),
    )
