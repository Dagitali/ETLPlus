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

# SECTION: INTERNAL CONSTANTS =============================================== #


_PANDAS_CACHE: dict[str, Any] = {}


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
    mod = _PANDAS_CACHE.get('mod')
    if mod is not None:  # pragma: no cover - tiny branch
        return mod
    try:
        _pd = __import__('pandas')  # type: ignore[assignment]
    except ImportError as e:  # pragma: no cover
        raise ImportError(
            f'{format_name} support requires optional dependency "pandas".\n'
            'Install with: pip install pandas',
        ) from e
    _PANDAS_CACHE['mod'] = _pd

    return _pd
