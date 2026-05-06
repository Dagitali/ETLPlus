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

from ..utils import JsonCodec
from ..utils._types import JSONData
from ._enums import FileFormat
from ._semi_structured_handlers import RecordPayloadTextCodecHandlerMixin

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'JsonFile',
]


# SECTION: CLASSES ========================================================== #


class JsonFile(RecordPayloadTextCodecHandlerMixin):
    """Handler implementation for JSON files."""

    # -- Class Attributes -- #

    format = FileFormat.JSON
    write_trailing_newline = True

    # -- Instance Methods -- #

    def decode_text_payload(
        self,
        text: str,
    ) -> object:
        """Parse raw JSON text into a Python payload."""
        return JsonCodec.decode(text)

    def encode_text_payload(
        self,
        data: JSONData,
    ) -> str:
        """Serialize *data* to JSON text."""
        return JsonCodec(compact=False, pretty=True).serialize(data)
