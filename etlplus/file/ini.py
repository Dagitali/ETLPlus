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

from ..types import JSONData
from ..types import JSONDict
from ._io import stringify_value
from .base import DictPayloadSemiStructuredTextFileHandlerABC
from .base import ReadOptions
from .base import WriteOptions
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
                parser['DEFAULT'] = {
                    key: stringify_value(value)
                    for key, value in values.items()
                }
            else:
                raise TypeError('INI DEFAULT section must be a dict')
            continue
        if not isinstance(values, dict):
            raise TypeError('INI sections must map to dicts')
        parser[section] = {
            key: stringify_value(value) for key, value in values.items()
        }
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
        raw_section = dict(parser.items(section))
        for key in defaults:
            raw_section.pop(key, None)
        payload[section] = raw_section
    return payload


# SECTION: CLASSES ========================================================== #


class IniFile(DictPayloadSemiStructuredTextFileHandlerABC):
    """
    Handler implementation for INI files.
    """

    # -- Class Attributes -- #

    format = FileFormat.INI

    # -- Instance Methods -- #

    def dumps_dict_payload(
        self,
        payload: JSONDict,
        *,
        options: WriteOptions | None = None,
    ) -> str:
        """
        Serialize dictionary *data* into INI text.

        Parameters
        ----------
        payload : JSONDict
            Dictionary payload to serialize.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        str
            Serialized INI text.
        """
        _ = options
        parser = _parser_from_payload(payload)

        from io import StringIO

        stream = StringIO()
        parser.write(stream)
        return stream.getvalue()

    def loads(
        self,
        text: str,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Parse INI *text* into dictionary payload.

        Parameters
        ----------
        text : str
            INI payload as text.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONData
            Parsed payload.
        """
        _ = options
        parser = configparser.ConfigParser()
        parser.read_string(text)
        return _payload_from_parser(parser)
