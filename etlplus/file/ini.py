"""
:mod:`etlplus.file.ini` module.

Helpers for reading/writing initialization (INI) files.

Notes
-----
- An INI file is a simple configuration file format that uses sections,
    properties, and values.
- Common cases:
    - Sections are denoted by square brackets (e.g., ``[section]``).
    - Properties are key-value pairs (e.g., ``key=value``).
    - Comments are often indicated by semicolons (``;``) or hash symbols
        (``#``).
- Rule of thumb:
    - If the file follows the INI specification, use this module for
        reading and writing.
"""

from __future__ import annotations

import configparser
from io import StringIO

from ..utils.types import JSONDict
from ._io import stringify_value
from ._semi_structured_handlers import DictPayloadTextCodecHandlerMixin
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'IniFile',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _parser_from_payload(
    payload: JSONDict,
) -> configparser.ConfigParser:
    """
    Build a ConfigParser instance from the JSON-like INI payload shape.

    Parameters
    ----------
    payload : JSONDict
        The INI payload as a dictionary.

    Returns
    -------
    configparser.ConfigParser
        The constructed ConfigParser instance.

    Raises
    ------
    TypeError
        If the payload structure is invalid.
    """
    parser = configparser.ConfigParser()
    for section, values in payload.items():
        if section == 'DEFAULT':
            if isinstance(values, dict):
                parser['DEFAULT'] = _stringify_mapping(values)
            else:
                raise TypeError('INI DEFAULT section must be a dict')
            continue
        if not isinstance(values, dict):
            raise TypeError('INI sections must map to dicts')
        parser[section] = _stringify_mapping(values)
    return parser


def _payload_from_parser(
    parser: configparser.ConfigParser,
) -> JSONDict:
    """
    Convert a ConfigParser instance to the JSON-like INI payload shape.

    Parameters
    ----------
    parser : configparser.ConfigParser
        The ConfigParser instance to convert.

    Returns
    -------
    JSONDict
        The JSON-like INI payload.
    """
    payload: JSONDict = {}
    if parser.defaults():
        payload['DEFAULT'] = dict(parser.defaults())
    defaults = dict(parser.defaults())
    for section in parser.sections():
        payload[section] = {
            key: value
            for key, value in parser.items(section)
            if key not in defaults
        }
    return payload


def _stringify_mapping(
    mapping: JSONDict,
) -> dict[str, str]:
    """
    Coerce one mapping payload into ``configparser`` string values.

    Parameters
    ----------
    mapping : JSONDict
        The mapping to stringify.

    Returns
    -------
    dict[str, str]
        The resulting mapping with stringified values.
    """
    return {key: stringify_value(value) for key, value in mapping.items()}


# SECTION: CLASSES ========================================================== #


class IniFile(DictPayloadTextCodecHandlerMixin):
    """Handler implementation for INI files."""

    # -- Class Attributes -- #

    format = FileFormat.INI

    # -- Instance Methods -- #

    def decode_dict_payload_text(
        self,
        text: str,
    ) -> object:
        """Parse INI *text* into dictionary payload."""
        parser = configparser.ConfigParser()
        parser.read_string(text)
        return _payload_from_parser(parser)

    def encode_dict_payload_text(
        self,
        payload: JSONDict,
    ) -> str:
        """Serialize dictionary *data* into INI text."""
        parser = _parser_from_payload(payload)

        stream = StringIO()
        parser.write(stream)
        return stream.getvalue()
