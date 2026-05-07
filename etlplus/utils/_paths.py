"""
:mod:`etlplus.utils._paths` module.

Path-oriented utility helpers.
"""

from __future__ import annotations

import hashlib
from os import PathLike
from pathlib import Path

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'PathHasher',
    'PathParser',
]


# SECTION: CLASSES ========================================================== #


class PathHasher:
    """Hash filesystem paths when they exist."""

    # -- Static Methods -- #

    @staticmethod
    def sha256(
        file_path: str | PathLike[str],
    ) -> str | None:
        """Return the SHA-256 digest for *file_path* when it is a file."""
        path = Path(file_path)
        if not path.is_file():
            return None
        digest = hashlib.sha256()
        with path.open('rb') as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b''):
                digest.update(chunk)
        return digest.hexdigest()


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
