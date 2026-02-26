"""
:mod:`etlplus.utils.text` module.

Text-processing utility helpers.
"""

from __future__ import annotations

from collections.abc import Callable
from collections.abc import Mapping

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions (text processing)
    'normalize_choice',
    'normalize_str',
]


# SECTION: FUNCTIONS ======================================================== #


def normalize_str(
    value: str | None,
) -> str:
    """
    Return lower-cased, trimmed text for normalization helpers.

    Parameters
    ----------
    value : str | None
        Optional user-provided text.

    Returns
    -------
    str
        Normalized string with surrounding whitespace removed and converted
        to lowercase. ``""`` when *value* is ``None``.
    """
    return (value or '').strip().lower()


def normalize_choice(
    value: str | None,
    *,
    mapping: Mapping[str, str],
    default: str,
    normalize: Callable[[str | None], str] = normalize_str,
) -> str:
    """
    Normalize a string choice using a mapping and fallback.

    Parameters
    ----------
    value : str | None
        Input value to normalize.
    mapping : Mapping[str, str]
        Mapping of acceptable normalized inputs to output values.
    default : str
        Default return value when input is missing or unrecognized.
    normalize : Callable[[str | None], str], optional
        Normalization function applied to *value*. Defaults to
        :func:`normalize_str`.

    Returns
    -------
    str
        Normalized mapped value or *default*.
    """
    return mapping.get(normalize(value), default)
