"""
etlplus.config.mixins
=====================

Lightweight mixins used by config models.
"""
from __future__ import annotations

from typing import Final


# SECTION: EXPORTS ========================================================== #


__all__ = ['BoundsWarningsMixin']


# SECTION: CLASSES ========================================================== #


class BoundsWarningsMixin:
    """
    Small helper for accumulating non-raising warnings.

    Usage
    -----
    >>> warnings: list[str] = []
    >>> BoundsWarningsMixin._warn_if(True, 'oops', warnings)
    >>> warnings
    ['oops']
    """

    __slots__ = ()

    _APPEND: Final = list.append

    # -- Static Methods -- #

    @staticmethod
    def _warn_if(
        condition: bool,
        message: str,
        bucket: list[str],
    ) -> None:
        """
        Append a warning message to a list if a condition is met.

        Parameters
        ----------
        condition : bool
            Whether to issue the warning.
        message : str
            The warning message.
        bucket : list[str]
            The list to append the warning message to.
        """

        if condition:
            BoundsWarningsMixin._APPEND(bucket, message)
