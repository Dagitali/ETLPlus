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

from ..types import JSONData
from ..types import JSONDict
from ..types import JSONList
from ..types import StrPath
from ..utils import count_records
from ._io import call_deprecated_module_read
from ._io import call_deprecated_module_write
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
    # Functions
    'read',
    'write',
]


# SECTION: CLASSES ========================================================== #


class NdjsonFile(SemiStructuredTextFileHandlerABC):
    """
    Handler implementation for NDJSON files.
    """

    # -- Class Attributes -- #

    format = FileFormat.NDJSON
    allow_dict_root = False
    allow_list_root = True

    # -- Instance Methods -- #

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
        encoding = self.encoding_from_read_options(options)
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
        encoding = self.encoding_from_write_options(options)
        write_text(
            path,
            self.dumps(rows, options=options),
            encoding=encoding,
        )
        return count_records(rows)

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

        Raises
        ------
        TypeError
            If any line in the NDJSON text is not a JSON object (dict).
        """
        _ = options
        rows: JSONList = []
        for idx, line in enumerate(text.splitlines(), start=1):
            stripped = line.strip()
            if not stripped:
                continue
            payload = json.loads(stripped)
            if not isinstance(payload, dict):
                raise TypeError(
                    f'NDJSON lines must be objects (dicts) (line {idx})',
                )
            rows.append(cast(JSONDict, payload))
        return rows

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
        _ = options
        rows = normalize_records(data, 'NDJSON')
        return ''.join(
            f'{json.dumps(row, ensure_ascii=False)}\n' for row in rows
        )


# SECTION: INTERNAL CONSTANTS =============================================== #

_NDJSON_HANDLER = NdjsonFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Deprecated wrapper. Use ``NdjsonFile().read(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the NDJSON file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the NDJSON file.
    """
    return cast(
        JSONList,
        call_deprecated_module_read(
            path,
            __name__,
            _NDJSON_HANDLER.read,
        ),
    )


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Deprecated wrapper. Use ``NdjsonFile().write(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the NDJSON file on disk.
    data : JSONData
        Data to write.

    Returns
    -------
    int
        Number of records written.
    """
    return call_deprecated_module_write(
        path,
        data,
        __name__,
        _NDJSON_HANDLER.write,
    )
