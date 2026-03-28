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

from ..utils.types import JSONDict
from ._enums import FileFormat
from ._io import stringify_value
from ._semi_structured_handlers import DictPayloadTextCodecHandlerMixin

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'PropertiesFile',
]


# SECTION: INTERNAL CONSTANTS =============================================== #


_SEPARATORS = ('=', ':')


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _parse_properties_text(
    text: str,
) -> JSONDict:
    """Parse Java-style properties text into key-value mappings."""
    payload: JSONDict = {}
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith(('#', '!')):
            continue

        key, value = _split_key_value(stripped)
        if key:
            payload[key] = value
    return payload


def _split_key_value(
    line: str,
) -> tuple[str, str]:
    """Split one normalized PROPERTIES line into ``(key, value)``."""
    separator_indexes = [line.find(sep) for sep in _SEPARATORS if sep in line]
    if not separator_indexes:
        return line, ''

    index = min(separator_indexes)
    return line[:index].strip(), line[index + 1:].strip()


# SECTION: CLASSES ========================================================== #


class PropertiesFile(DictPayloadTextCodecHandlerMixin):
    """Handler implementation for Java-style properties files."""

    # -- Class Attributes -- #

    format = FileFormat.PROPERTIES

    # -- Instance Methods -- #

    def decode_dict_payload_text(
        self,
        text: str,
    ) -> object:
        """Parse PROPERTIES *text* into dictionary payload."""
        return _parse_properties_text(text)

    def encode_dict_payload_text(
        self,
        payload: JSONDict,
    ) -> str:
        """Serialize dictionary *data* into PROPERTIES text."""
        return ''.join(
            f'{key}={stringify_value(value)}\n'
            for key, value in sorted(payload.items())
        )
