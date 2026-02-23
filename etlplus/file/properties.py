"""
:mod:`etlplus.file.properties` module.

Helpers for reading/writing properties (PROPERTIES) files.

Notes
-----
- A PROPERTIES file is a properties file that typically uses key-value pairs,
    often with a simple syntax.
- Common cases:
    - Java-style properties files with ``key=value`` pairs.
    - INI-style files without sections.
    - Custom formats specific to certain applications.
- Rule of thumb:
    - If the file follows a standard format like INI, consider using
        dedicated parsers.
"""

from __future__ import annotations

from ..types import JSONData
from ..types import JSONDict
from ._io import make_deprecated_module_io
from ._io import stringify_value
from .base import DictPayloadSemiStructuredTextFileHandlerABC
from .base import ReadOptions
from .base import WriteOptions
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'PropertiesFile',
    # Functions
    'read',
    'write',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _parse_properties_text(
    text: str,
) -> JSONDict:
    """
    Parse Java-style properties text into key-value mappings.
    """
    payload: JSONDict = {}
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith(('#', '!')):
            continue
        separator_index = -1
        for sep in ('=', ':'):
            if sep in stripped:
                separator_index = stripped.find(sep)
                break
        if separator_index == -1:
            key = stripped
            value = ''
        else:
            key = stripped[:separator_index].strip()
            value = stripped[separator_index + 1:].strip()
        if key:
            payload[key] = value
    return payload


# SECTION: CLASSES ========================================================== #


class PropertiesFile(DictPayloadSemiStructuredTextFileHandlerABC):
    """
    Handler implementation for Java-style properties files.
    """

    # -- Class Attributes -- #

    format = FileFormat.PROPERTIES

    # -- Instance Methods -- #

    def dumps_dict_payload(
        self,
        payload: JSONDict,
        *,
        options: WriteOptions | None = None,
    ) -> str:
        """
        Serialize dictionary *data* into PROPERTIES text.

        Parameters
        ----------
        payload : JSONDict
            Dictionary payload to serialize.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        str
            Serialized PROPERTIES text.
        """
        _ = options
        return ''.join(
            f'{key}={stringify_value(payload[key])}\n'
            for key in sorted(payload.keys())
        )

    def loads(
        self,
        text: str,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Parse PROPERTIES *text* into dictionary payload.

        Parameters
        ----------
        text : str
            PROPERTIES payload as text.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONData
            Parsed payload.
        """
        _ = options
        return _parse_properties_text(text)


# SECTION: INTERNAL CONSTANTS =============================================== #

_PROPERTIES_HANDLER = PropertiesFile()


# SECTION: FUNCTIONS ======================================================== #


read, write = make_deprecated_module_io(__name__, _PROPERTIES_HANDLER)
