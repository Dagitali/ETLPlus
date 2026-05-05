"""
:mod:`etlplus.utils._text` module.

Text-processing utility helpers.
"""

from __future__ import annotations

from collections.abc import Callable
from collections.abc import Mapping

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'TextNormalizer',
]


# SECTION: CLASSES ========================================================== #


class TextNormalizer:
    """Normalize user-provided text and text-backed choices."""

    # -- Static Methods -- #

    @staticmethod
    def normalize(
        value: str | None,
    ) -> str:
        """
        Return case-folded, trimmed text for normalization helpers.

        Parameters
        ----------
        value : str | None
            Optional user-provided text.

        Returns
        -------
        str
            Normalized string with surrounding whitespace removed and case-
            folded. ``""`` when *value* is ``None``.
        """
        return (value or '').strip().casefold()

    # -- Class Methods -- #

    @classmethod
    def resolve_choice(
        cls,
        value: str | None,
        *,
        mapping: Mapping[str, str],
        default: str,
        normalize: Callable[[str | None], str] | None = None,
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
        normalize : Callable[[str | None], str] | None, optional
            Normalization function applied to *value*. Defaults to
            :meth:`normalize`.

        Returns
        -------
        str
            Normalized mapped value or *default*.
        """
        normalizer = normalize or cls.normalize
        return mapping.get(normalizer(value), default)
