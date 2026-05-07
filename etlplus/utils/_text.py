"""
:mod:`etlplus.utils._text` module.

Text-processing utility helpers.
"""

from __future__ import annotations

from collections.abc import Callable
from collections.abc import Mapping
from dataclasses import dataclass

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'TextChoiceResolver',
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

    @staticmethod
    def truncate(
        text: str | None,
        *,
        limit: int,
    ) -> str:
        """
        Return *text* shortened to *limit* characters.

        Parameters
        ----------
        text : str | None
            Text to shorten.
        limit : int
            Maximum number of characters to keep.

        Returns
        -------
        str
            Truncated text, or ``""`` when *text* is falsey.
        """
        if not text:
            return ''
        return text[:limit]


# SECTION: DATA CLASSES ===================================================== #


@dataclass(frozen=True, slots=True)
class TextChoiceResolver:
    """
    Resolve normalized text choices using one mapping and fallback.

    Attributes
    ----------
    mapping : Mapping[str, str]
        Mapping of acceptable normalized inputs to output values.
    default : str
        Fallback returned when input is missing or unrecognized.
    normalize : Callable[[str | None], str]
        Function applied to incoming values before lookup.
    """

    # -- Instance Attributes -- #

    mapping: Mapping[str, str]
    default: str
    normalize: Callable[[str | None], str] = TextNormalizer.normalize

    # -- Instance Methods -- #

    def resolve(
        self,
        value: str | None,
    ) -> str:
        """
        Return the mapped choice for *value* or the configured fallback.

        Parameters
        ----------
        value : str | None
            Input value to normalize.

        Returns
        -------
        str
            Normalized mapped value or configured fallback.
        """
        return self.mapping.get(self.normalize(value), self.default)
