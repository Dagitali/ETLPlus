"""
:mod:`etlplus.file.ndjson` module.

Helpers for reading/writing Newline Delimited JSON (NDJSON) files.

Notes
-----
- An NDJSON file is a format where each line is a separate JSON object.
- Common cases:
    - Streaming JSON data.
    - Log files with JSON entries.
    - Large datasets that are processed line-by-line.
- Rule of thumb:
    - If the file follows the NDJSON specification, use this module for
        reading and writing.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import cast

from ..utils import count_records
from ..utils.types import JSONData
from ..utils.types import JSONDict
from ..utils.types import JSONList
from ._io import normalize_records
from ._io import read_text
from ._io import write_text
from .base import ReadOptions
from .base import SemiStructuredTextFileHandlerABC
from .base import WriteOptions
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'NdjsonFile',
]


# SECTION: CLASSES ========================================================== #


class NdjsonFile(SemiStructuredTextFileHandlerABC):
    """Handler implementation for NDJSON files."""

    # -- Class Attributes -- #

    format = FileFormat.NDJSON
    allow_dict_root = False
    allow_list_root = True

    # -- Instance Methods -- #

    def dump_line(
        self,
        data: JSONDict,
        *,
        options: WriteOptions | None = None,
    ) -> str:
        """
        Serialize one dictionary record as a single NDJSON line.

        Parameters
        ----------
        data : JSONDict
            One JSON object record to serialize.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        str
            Serialized NDJSON line including the trailing newline.
        """
        _ = options
        return f'{json.dumps(data, ensure_ascii=False)}\n'

    def dumps(
        self,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> str:
        """
        Serialize *data* to NDJSON text.

        Parameters
        ----------
        data : JSONData
            Payload to serialize.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        str
            Serialized NDJSON text.
        """
        rows = normalize_records(data, 'NDJSON')
        return ''.join(self.dump_line(row, options=options) for row in rows)

    def load_line(
        self,
        text: str,
        *,
        options: ReadOptions | None = None,
        line_number: int | None = None,
    ) -> JSONDict:
        """
        Parse one NDJSON record line into a dictionary.

        Parameters
        ----------
        text : str
            One NDJSON record line.
        options : ReadOptions | None, optional
            Optional read parameters.
        line_number : int | None, optional
            Optional source line number used for error reporting.

        Returns
        -------
        JSONDict
            Parsed JSON object for the line.

        Raises
        ------
        ValueError
            If the input line is blank.
        TypeError
            If the line does not contain a JSON object (dict).
        """
        _ = options
        stripped = text.strip()
        if not stripped:
            raise ValueError('NDJSON line cannot be blank')
        payload = json.loads(stripped)
        if not isinstance(payload, dict):
            suffix = f' (line {line_number})' if line_number is not None else ''
            raise TypeError(f'NDJSON lines must be objects (dicts){suffix}')
        return cast(JSONDict, payload)

    def loads(
        self,
        text: str,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Parse NDJSON *text* into a list of records.

        Parameters
        ----------
        text : str
            NDJSON payload as text.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONData
            Parsed records.
        """
        rows: JSONList = []
        for idx, line in enumerate(text.splitlines(), start=1):
            stripped = line.strip()
            if not stripped:
                continue
            rows.append(self.load_line(stripped, options=options, line_number=idx))
        return rows

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read and return NDJSON content from *path*.

        Parameters
        ----------
        path : Path
            Path to the NDJSON file on disk.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONList
            The list of dictionaries read from the NDJSON file.
        """
        encoding = self.encoding_from_options(options)
        return cast(
            JSONList,
            self.loads(read_text(path, encoding=encoding), options=options),
        )

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write *data* to NDJSON at *path* and return record count.

        Parameters
        ----------
        path : Path
            Path to the NDJSON file on disk.
        data : JSONData
            Data to write.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        int
            Number of records written.
        """
        rows = normalize_records(data, 'NDJSON')
        if not rows:
            return 0
        encoding = self.encoding_from_options(options)
        write_text(
            path,
            self.dumps(rows, options=options),
            encoding=encoding,
        )
        return count_records(rows)
