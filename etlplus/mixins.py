"""
:mod:`etlplus.mixins` module.

Shared mixin utilities used across configuration and API layers.

Notes
------
- Mixins are stateless helpers.
- ``__slots__`` prevents accidental attribute mutation at runtime.
"""

from __future__ import annotations

from typing import Final

# SECTION: EXPORTS ========================================================== #


__all__ = ['BoundsWarningsMixin']


# SECTION: EXPORTS ========================================================== #


class BoundsWarningsMixin:
    """
    Append human-readable warnings without raising exceptions.

    Examples
    --------
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
        Append a warning to a list if a condition is met.

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
