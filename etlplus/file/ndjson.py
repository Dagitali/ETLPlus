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
from ._io import coerce_path
from ._io import ensure_parent_dir
from ._io import normalize_records
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

        Raises
        ------
        TypeError
            If any line in the NDJSON file is not a JSON object (dict).
        """
        encoding = options.encoding if options is not None else 'utf-8'
        rows: JSONList = []
        with path.open('r', encoding=encoding) as handle:
            for idx, line in enumerate(handle, start=1):
                text = line.strip()
                if not text:
                    continue
                payload = json.loads(text)
                if not isinstance(payload, dict):
                    raise TypeError(
                        f'NDJSON lines must be objects (dicts) (line {idx})',
                    )
                rows.append(cast(JSONDict, payload))
        return rows

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
        encoding = options.encoding if options is not None else 'utf-8'
        ensure_parent_dir(path)
        with path.open('w', encoding=encoding) as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False))
                handle.write('\n')
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


# SECTION: INTERNAL CONSTANTS ============================================== #


_NDJSON_HANDLER = NdjsonFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Read and return NDJSON content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the NDJSON file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the NDJSON file.
    """
    return cast(JSONList, _NDJSON_HANDLER.read(coerce_path(path)))


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Write *data* to NDJSON at *path* and return record count.

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
    return _NDJSON_HANDLER.write(coerce_path(path), data)
