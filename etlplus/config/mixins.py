"""
:mod:`etlplus.config.mixins` module.

Lightweight mixins used by configuration models.

Notes
-----
- Mixins are stateless and declare ``__slots__ = ()`` to avoid accidental
    attribute creation.
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
        """Append a warning to a list if a condition is met.

        Parameters
        ----------
        condition : bool
            Whether to issue the warning.
        message : str
            Warning message to append.
        bucket : list[str]
            Target list for collected warnings.
        """
        if condition:
            BoundsWarningsMixin._APPEND(bucket, message)
