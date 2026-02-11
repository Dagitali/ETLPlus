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
from pathlib import Path

from ..types import JSONData
from ..types import JSONDict
from ..types import StrPath
from ._io import coerce_path
from ._io import read_text
from ._io import require_dict_payload
from ._io import stringify_value
from ._io import warn_deprecated_module_io
from ._io import write_text
from .base import ReadOptions
from .base import SemiStructuredTextFileHandlerABC
from .base import WriteOptions
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'IniFile',
    # Functions
    'read',
    'write',
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


class IniFile(SemiStructuredTextFileHandlerABC):
    """
    Handler implementation for INI files.
    """

    # -- Class Attributes -- #

    format = FileFormat.INI
    allow_dict_root = True
    allow_list_root = False

    # -- Instance Methods -- #

    def dumps(
        self,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> str:
        """
        Serialize dictionary *data* into INI text.

        Parameters
        ----------
        data : JSONData
            Payload to serialize.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        str
            Serialized INI text.
        """
        _ = options
        payload = require_dict_payload(data, format_name='INI')
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

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Read and return INI content from *path*.

        Parameters
        ----------
        path : Path
            Path to the INI file on disk.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONData
            The structured data read from the INI file.
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
        Write *data* to INI at *path* and return record count.

        Parameters
        ----------
        path : Path
            Path to the INI file on disk.
        data : JSONData
            Data to write as INI. Should be a dictionary.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        int
            The number of records written to the INI file.
        """
        encoding = self.encoding_from_write_options(options)
        write_text(
            path,
            self.dumps(data, options=options),
            encoding=encoding,
        )
        return 1


# SECTION: INTERNAL CONSTANTS =============================================== #

_INI_HANDLER = IniFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONData:
    """
    Read and return INI content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the INI file on disk.

    Returns
    -------
    JSONData
        The structured data read from the INI file.
    """
    warn_deprecated_module_io(__name__, 'read')
    return _INI_HANDLER.read(coerce_path(path))


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Write *data* to INI at *path* and return record count.

    Parameters
    ----------
    path : StrPath
        Path to the INI file on disk.
    data : JSONData
        Data to write as INI. Should be a dictionary.

    Returns
    -------
    int
        The number of records written to the INI file.
    """
    warn_deprecated_module_io(__name__, 'write')
    return _INI_HANDLER.write(coerce_path(path), data)
