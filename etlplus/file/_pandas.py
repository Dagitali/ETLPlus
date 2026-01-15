"""
:mod:`etlplus.file._pandas` module.

Shared helpers for optional pandas usage.
"""

from __future__ import annotations

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
    from ._imports import get_optional_module

    return get_optional_module(
        'pandas',
        error_message=(
            f'{format_name} support requires optional dependency "pandas".\n'
            'Install with: pip install pandas'
        ),
    )
