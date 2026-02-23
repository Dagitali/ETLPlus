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

from ..types import JSONData
from ._io import make_deprecated_module_io
from .base import ReadOptions
from .base import RecordPayloadSemiStructuredTextFileHandlerABC
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


class JsonFile(RecordPayloadSemiStructuredTextFileHandlerABC):
    """
    Handler implementation for JSON files.
    """

    # -- Class Attributes -- #

    format = FileFormat.JSON
    write_trailing_newline = True

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

    def loads_payload(
        self,
        text: str,
        *,
        options: ReadOptions | None = None,
    ) -> object:
        """
        Parse raw JSON text into a Python payload.

        Parameters
        ----------
        text : str
            JSON payload as text.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        object
            Parsed payload.
        """
        _ = options
        return json.loads(text)


# SECTION: INTERNAL CONSTANTS =============================================== #

_JSON_HANDLER = JsonFile()


# SECTION: FUNCTIONS ======================================================== #


read, write = make_deprecated_module_io(__name__, _JSON_HANDLER)
