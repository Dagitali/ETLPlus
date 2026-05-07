"""
:mod:`etlplus.utils._paths` module.

Path-oriented utility helpers.
"""

from __future__ import annotations

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'PathParser',
]


# SECTION: CLASSES ========================================================== #


class PathParser:
    """Parse and classify path-like strings."""

    # -- Static Methods -- #

    @staticmethod
    def is_windows_drive_path(
        value: str,
    ) -> bool:
        """
        Return whether *value* begins with a Windows drive prefix.

        Parameters
        ----------
        value : str
            Path string to inspect.

        Returns
        -------
        bool
            ``True`` when *value* starts with a drive prefix such as ``C:``.
        """
        return len(value) >= 2 and value[0].isalpha() and value[1] == ':'
