"""
:mod:`etlplus.utils._paths` module.

Path-oriented utility helpers.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from os import PathLike
from pathlib import Path
from typing import TypeGuard

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'PathHasher',
    'PathParser',
]


# SECTION: DATA CLASSES ===================================================== #


@dataclass(frozen=True, slots=True)
class PathHasher:
    """
    Hash one filesystem path when it exists.

    Attributes
    ----------
    file_path : str | PathLike[str]
        File path to hash.
    """

    # -- Instance Attributes -- #

    file_path: str | PathLike[str]

    # -- Instance Methods -- #

    def sha256(self) -> str | None:
        """Return the SHA-256 digest for this path when it is a file."""
        path = Path(self.file_path)
        if not path.is_file():
            return None
        with path.open('rb') as handle:
            return hashlib.file_digest(handle, 'sha256').hexdigest()


# SECTION: CLASSES ========================================================== #


class PathParser:
    """Parse and classify path-like strings."""

    # -- Static Methods -- #

    @staticmethod
    def is_file_target(
        value: object,
    ) -> TypeGuard[str | PathLike[str]]:
        """
        Return whether *value* names a concrete file target.

        Parameters
        ----------
        value : object
            Output destination supplied by a caller.

        Returns
        -------
        TypeGuard[str | PathLike[str]]
            ``True`` when *value* is path-like and does not represent STDOUT.
        """
        return isinstance(value, str | PathLike) and not PathParser.is_stdout_target(
            value,
        )

    @staticmethod
    def is_stdout_target(
        value: object,
    ) -> bool:
        """
        Return whether *value* represents STDOUT.

        Parameters
        ----------
        value : object
            Output destination supplied by a caller.

        Returns
        -------
        bool
            ``True`` for ``None``, blank strings, or ``"-"`` with surrounding
            whitespace.
        """
        return value is None or (isinstance(value, str) and value.strip() in {'', '-'})

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
