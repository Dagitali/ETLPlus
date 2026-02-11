"""
:mod:`etlplus.file.json` module.

Helpers for reading/writing JavaScript Object Notation (JSON) files.

Notes
-----
- A JSON file is a widely used data interchange format that uses
    human-readable text to represent structured data.
- Common cases:
    - Data interchange between web applications and servers.
    - Configuration files for applications.
    - Data storage for NoSQL databases.
- Rule of thumb:
    - If the file follows the JSON specification, use this module for
        reading and writing.
"""

from __future__ import annotations

import json
from pathlib import Path

from ..types import JSONData
from ..types import StrPath
from ..utils import count_records
from ._io import coerce_path
from ._io import coerce_record_payload
from ._io import read_text
from ._io import warn_deprecated_module_io
from ._io import write_text
from .base import ReadOptions
from .base import SemiStructuredTextFileHandlerABC
from .base import WriteOptions
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'JsonFile',
    # Functions
    'read',
    'write',
]


# SECTION: CLASSES ========================================================== #


class JsonFile(SemiStructuredTextFileHandlerABC):
    """
    Handler implementation for JSON files.
    """

    # -- Class Attributes -- #

    format = FileFormat.JSON

    # -- Instance Methods -- #

    def dumps(
        self,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> str:
        """
        Serialize *data* to JSON text.

        Parameters
        ----------
        data : JSONData
            Payload to serialize.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        str
            Serialized JSON text.
        """
        _ = options
        return json.dumps(data, indent=2, ensure_ascii=False)

    def loads(
        self,
        text: str,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Parse JSON *text* into structured records.

        Parameters
        ----------
        text : str
            JSON payload as text.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONData
            Parsed payload.
        """
        _ = options
        return coerce_record_payload(json.loads(text), format_name='JSON')

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Read and return JSON content from *path*.

        Parameters
        ----------
        path : Path
            Path to the JSON file on disk.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONData
            The structured data read from the JSON file.
        """
        encoding = self.encoding_from_read_options(options)
        return self.loads(
            read_text(path, encoding=encoding),
            options=options,
        )

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write *data* to JSON at *path* and return record count.

        Parameters
        ----------
        path : Path
            Path to the JSON file on disk.
        data : JSONData
            Data to serialize as JSON.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        int
            The number of records written to the JSON file.
        """
        encoding = self.encoding_from_write_options(options)
        write_text(
            path,
            self.dumps(data, options=options),
            encoding=encoding,
            trailing_newline=True,
        )
        return count_records(data)


# SECTION: INTERNAL CONSTANTS =============================================== #

_JSON_HANDLER = JsonFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONData:
    """
    Read and return JSON content from *path*.

    Validates that the JSON root is a dict or a list of dicts.

    Parameters
    ----------
    path : StrPath
        Path to the JSON file on disk.

    Returns
    -------
    JSONData
        The structured data read from the JSON file.
    """
    warn_deprecated_module_io(__name__, 'read')
    return _JSON_HANDLER.read(coerce_path(path))


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Write *data* to JSON at *path* and return record count.

    Parameters
    ----------
    path : StrPath
        Path to the JSON file on disk.
    data : JSONData
        Data to serialize as JSON.

    Returns
    -------
    int
        The number of records written to the JSON file.
    """
    warn_deprecated_module_io(__name__, 'write')
    return _JSON_HANDLER.write(coerce_path(path), data)
