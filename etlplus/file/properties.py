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

from pathlib import Path

from ..types import JSONData
from ..types import JSONDict
from ..types import StrPath
from ._io import coerce_path
from ._io import ensure_parent_dir
from ._io import require_dict_payload
from ._io import stringify_value
from .base import ReadOptions
from .base import SemiStructuredTextFileHandlerABC
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


class PropertiesFile(SemiStructuredTextFileHandlerABC):
    """
    Handler implementation for Java-style properties files.
    """

    # -- Class Attributes -- #

    format = FileFormat.PROPERTIES
    allow_dict_root = True
    allow_list_root = False

    # -- Instance Methods -- #

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Read and return PROPERTIES content from *path*.

        Parameters
        ----------
        path : Path
            Path to the PROPERTIES file on disk.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONData
            The structured data read from the PROPERTIES file.
        """
        encoding = self.encoding_from_read_options(options)
        return _parse_properties_text(path.read_text(encoding=encoding))

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write *data* to PROPERTIES at *path* and return record count.

        Parameters
        ----------
        path : Path
            Path to the PROPERTIES file on disk.
        data : JSONData
            Data to write as PROPERTIES. Should be a dictionary.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        int
            The number of records written to the PROPERTIES file.
        """
        encoding = self.encoding_from_write_options(options)
        payload = require_dict_payload(data, format_name='PROPERTIES')
        ensure_parent_dir(path)
        with path.open('w', encoding=encoding, newline='') as handle:
            for key in sorted(payload.keys()):
                handle.write(f'{key}={stringify_value(payload[key])}\n')
        return 1

    def dumps(
        self,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> str:
        """
        Serialize dictionary *data* into PROPERTIES text.

        Parameters
        ----------
        data : JSONData
            Payload to serialize.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        str
            Serialized PROPERTIES text.
        """
        _ = options
        payload = require_dict_payload(data, format_name='PROPERTIES')
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


def read(
    path: StrPath,
) -> JSONData:
    """
    Read and return PROPERTIES content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the PROPERTIES file on disk.

    Returns
    -------
    JSONData
        The structured data read from the PROPERTIES file.
    """
    return _PROPERTIES_HANDLER.read(coerce_path(path))


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Write *data* to PROPERTIES at *path* and return record count.

    Parameters
    ----------
    path : StrPath
        Path to the PROPERTIES file on disk.
    data : JSONData
        Data to write as PROPERTIES. Should be a dictionary.

    Returns
    -------
    int
        The number of records written to the PROPERTIES file.
    """
    return _PROPERTIES_HANDLER.write(coerce_path(path), data)
