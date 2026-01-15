"""
:mod:`etlplus.file._pandas` module.

Shared helpers for optional pandas usage.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'get_pandas',
]

# SECTION: FUNCTIONS ======================================================== #


def get_pandas(format_name: str) -> Any:
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

    Raises
    ------
    ImportError
        If the optional dependency is missing.
    """
    try:
        return import_module('pandas')
    except ImportError as e:  # pragma: no cover
        raise ImportError(
            f'{format_name} support requires optional dependency "pandas".\n'
            'Install with: pip install pandas',
        ) from e
