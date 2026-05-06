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


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _normalize_text(value: str | None) -> str:
    """Return case-folded, trimmed text."""
    return (value or '').strip().casefold()


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
    normalize: Callable[[str | None], str] = _normalize_text

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
        return _normalize_text(value)

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
        return TextChoiceResolver(
            mapping=mapping,
            default=default,
            normalize=normalize or cls.normalize,
        ).resolve(value)
