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

from ..utils import stringify_value
from ..utils._types import JSONDict
from ._enums import FileFormat
from ._semi_structured_handlers import DictPayloadTextCodecHandlerMixin

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'PropertiesFile',
]


# SECTION: INTERNAL CONSTANTS =============================================== #


_SEPARATORS = ('=', ':')


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _first_properties_separator_index(
    line: str,
) -> int | None:
    """Return the first unescaped PROPERTIES separator index."""
    for index, char in enumerate(line):
        if (
            char in _SEPARATORS
            or char.isspace()
        ) and not _is_escaped(line, index):
            return index
    return None


def _is_escaped(
    line: str,
    index: int,
) -> bool:
    """Return whether the character at *index* is escaped by backslashes."""
    backslash_count = 0
    cursor = index - 1
    while cursor >= 0 and line[cursor] == '\\':
        backslash_count += 1
        cursor -= 1
    return backslash_count % 2 == 1


def _is_logical_line_continued(
    line: str,
) -> bool:
    """Return whether *line* continues onto the next physical line."""
    backslash_count = 0
    cursor = len(line) - 1
    while cursor >= 0 and line[cursor] == '\\':
        backslash_count += 1
        cursor -= 1
    return backslash_count % 2 == 1


def _iter_logical_property_lines(
    text: str,
) -> list[str]:
    """Return PROPERTIES logical lines after joining continuations."""
    lines: list[str] = []
    pending = ''
    continuing = False

    for physical_line in text.splitlines():
        line = physical_line.lstrip() if continuing else physical_line
        if _is_logical_line_continued(line):
            pending += line[:-1]
            continuing = True
            continue

        lines.append(f'{pending}{line}' if continuing else line)
        pending = ''
        continuing = False

    if continuing:
        lines.append(pending)

    return lines


def _parse_properties_text(
    text: str,
) -> JSONDict:
    """Parse Java-style properties text into key-value mappings."""
    payload: JSONDict = {}
    for line in _iter_logical_property_lines(text):
        stripped = line.strip()
        if not stripped or stripped.startswith(('#', '!')):
            continue

        key, value = _split_key_value(stripped)
        if key:
            payload[key] = value
    return payload


def _skip_property_value_prefix(
    line: str,
    separator_index: int,
) -> int:
    """Skip whitespace and optional ``=``/``:`` before a property value."""
    value_index = separator_index
    while value_index < len(line) and line[value_index].isspace():
        value_index += 1
    if value_index < len(line) and line[value_index] in _SEPARATORS:
        value_index += 1
    while value_index < len(line) and line[value_index].isspace():
        value_index += 1
    return value_index


def _split_key_value(
    line: str,
) -> tuple[str, str]:
    """Split one normalized PROPERTIES line into ``(key, value)``."""
    separator_index = _first_properties_separator_index(line)
    if separator_index is None:
        return line, ''

    if line[separator_index] in _SEPARATORS:
        return line[:separator_index].strip(), line[separator_index + 1:].strip()

    value_index = _skip_property_value_prefix(line, separator_index)
    return line[:separator_index].strip(), line[value_index:].strip()


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
