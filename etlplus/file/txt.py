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

from collections.abc import Mapping
from pathlib import Path

from ._io import read_text
from ._io import write_text
from .base import PlainTextFileHandlerABC
from .base import ReadOptions
from .base import WriteOptions
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'TxtFile',
]

# SECTION: INTERNAL FUNCTIONS =============================================== #


def _count_text_lines(
    text: str,
) -> int:
    """
    Return the number of logical lines contained in *text*.

    Parameters
    ----------
    text : str
        Text to count lines in.

    Returns
    -------
    int
        Number of logical lines in the text. Lines are delimited by newline.
    """
    return len(text.splitlines())


def _legacy_text_value(
    payload: object,
) -> str | None:
    """
    Return one legacy ``{"text": "..."}`` value when present.

    This supports legacy record payloads for TXT writes. The presence of a
    ``"text"`` key with a string value indicates a legacy payload, and the
    string value is returned. Otherwise, ``None`` is returned to indicate no
    legacy text.

    Parameters
    ----------
    payload : object
        Potential legacy record payload to extract text from.

    Returns
    -------
    str | None
        The legacy text value if present, or ``None`` if not found.
    """
    if not isinstance(payload, Mapping):
        return None
    value = payload.get('text')
    return value if isinstance(value, str) else None


def _coerce_text_payload(
    data: object,
) -> str:
    """
    Normalize TXT write payloads into plain text.

    Plain text writes prefer ``str`` or ``list[str]`` payloads. Legacy
    ``{"text": "..."}`` record payloads remain accepted for compatibility.

    Parameters
    ----------
    data : object
        The original TXT write payload, which may be a raw string, a list of
        strings, or a legacy record.

    Returns
    -------
    str
        The normalized plain text payload to write to the file.
    """
    if isinstance(data, str):
        return data

    if (legacy_text := _legacy_text_value(data)) is not None:
        return legacy_text

    if not isinstance(data, list):
        raise TypeError(
            'TXT payloads must be raw text, a list of strings, or legacy '
            '{"text": "..."} records',
        )

    if all(isinstance(line, str) for line in data):
        return '\n'.join(data)

    legacy_lines: list[str] = []
    for item in data:
        if (legacy_text := _legacy_text_value(item)) is None:
            raise TypeError(
                'TXT payload lists must contain only strings or '
                '{"text": "..."} records',
            )
        legacy_lines.append(legacy_text)
    return '\n'.join(legacy_lines)


# SECTION: CLASSES ========================================================== #


class TxtFile(PlainTextFileHandlerABC):
    """
    Handler implementation for TXT files.
    """

    # -- Class Attributes -- #

    format = FileFormat.TXT

    # -- Instance Methods -- #

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> str:
        """
        Read and return raw TXT content at *path*.

        Parameters
        ----------
        path : Path
            Path to the TXT file on disk.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        str
            Raw text content read from the TXT file.
        """
        encoding = self.encoding_from_options(
            options,
            default=self.default_encoding,
        )
        return read_text(path, encoding=encoding)

    def read_rows(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> list[str]:
        """
        Read TXT content at *path* as plain text lines.

        Parameters
        ----------
        path : Path
            Path to the TXT file on disk.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        list[str]
            Text lines read from the TXT file. Blank lines are preserved.
        """
        return self.read(path, options=options).splitlines()

    def write(
        self,
        path: Path,
        data: object,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write plain text content to TXT at *path* and return line count.

        Parameters
        ----------
        path : Path
            Path to the TXT file on disk.
        data : object
            Text payload to write. Accepts raw strings, lists of strings,
            and legacy ``{"text": "..."}`` record payloads.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        int
            Number of logical lines written to the file.
        """
        encoding = self.encoding_from_options(
            options,
            default=self.default_encoding,
        )
        payload = _coerce_text_payload(data)
        write_text(path, payload, encoding=encoding)
        return _count_text_lines(payload)

    def write_rows(
        self,
        path: Path,
        rows: list[str],
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write text lines to TXT at *path* and return line count.

        Parameters
        ----------
        path : Path
            Path to the TXT file on disk.
        rows : list[str]
            Text lines to write.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        int
            Number of logical lines written to the file.
        """
        if not all(isinstance(row, str) for row in rows):
            raise TypeError('TXT row payloads must contain only strings')
        return self.write(path, rows, options=options)
