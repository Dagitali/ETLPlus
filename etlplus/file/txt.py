"""
:mod:`etlplus.file.txt` module.

Helpers for reading/writing text (TXT) files.

Notes
-----
- A TXT file is a plain text file that contains unformatted text.
- Common cases:
    - Each line in the file represents a single piece of text.
    - Lines may vary in length and content.
- Rule of thumb:
    - If the file is a simple text file without specific formatting
        requirements, use this module for reading and writing.
"""

from __future__ import annotations

from pathlib import Path

from ..types import JSONData
from ..types import JSONList
from ..types import StrPath
from ..utils import count_records
from ._io import call_deprecated_module_read
from ._io import call_deprecated_module_write
from ._io import read_text
from ._io import write_text
from .base import ReadOptions
from .base import TextFixedWidthFileHandlerABC
from .base import WriteOptions
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'TxtFile',
    # Functions
    'read',
    'write',
]

# SECTION: CLASSES ========================================================== #


class TxtFile(TextFixedWidthFileHandlerABC):
    """
    Handler implementation for TXT files.
    """

    # -- Class Attributes -- #

    format = FileFormat.TXT

    # -- Instance Methods -- #

    def read_rows(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read row records from TXT content at *path*.

        Parameters
        ----------
        path : Path
            Path to the TXT file on disk.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONList
            The list of dictionaries parsed from the TXT file.
        """
        encoding = self.encoding_from_read_options(
            options,
            default=self.default_encoding,
        )
        return [
            {'text': line}
            for line in read_text(path, encoding=encoding).splitlines()
            if line != ''
        ]

    def write_rows(
        self,
        path: Path,
        rows: JSONList,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write row records to TXT at *path* and return record count.

        Parameters
        ----------
        path : Path
            Path to the TXT file on disk.
        rows : JSONList
            Rows to write. Expects ``{'text': '...'} `` records.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        int
            Number of records written.

        Raises
        ------
        TypeError
            If any dictionary
            does not contain a ``'text'`` key.
        """
        if not rows:
            return 0

        encoding = self.encoding_from_write_options(
            options,
            default=self.default_encoding,
        )
        for row in rows:
            if 'text' not in row:
                raise TypeError('TXT payloads must include a "text" key')
        payload = ''.join(f'{row["text"]}\n' for row in rows)
        write_text(path, payload, encoding=encoding)
        return count_records(rows)


# SECTION: INTERNAL CONSTANTS =============================================== #

_TXT_HANDLER = TxtFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Deprecated wrapper. Use ``TxtFile().read(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the TXT file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the TXT file.
    """
    return call_deprecated_module_read(
        path,
        __name__,
        _TXT_HANDLER.read,
    )


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Deprecated wrapper. Use ``TxtFile().write(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the TXT file on disk.
    data : JSONData
        Data to write. Expects ``{'text': '...'} `` or a list of those.

    Returns
    -------
    int
        Number of records written.
    """
    return call_deprecated_module_write(
        path,
        data,
        __name__,
        _TXT_HANDLER.write,
    )
