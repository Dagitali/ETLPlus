"""
:mod:`etlplus.file.yaml` module.

Helpers for reading/writing YAML Ain't Markup Language (YAML) files.

Notes
-----
- A YAML file is a human-readable data serialization format.
- Common cases:
    - Configuration files.
    - Data exchange between languages with different data structures.
    - Complex data storage.
- Rule of thumb:
    - If the file follows the YAML specification, use this module for
        reading and writing.
"""

from __future__ import annotations

from ..types import JSONData
from ..types import StrPath
from ._imports import get_yaml
from ._io import call_deprecated_module_read
from ._io import call_deprecated_module_write
from ._io import coerce_record_payload
from .base import ReadOptions
from .base import SemiStructuredTextFileHandlerABC
from .base import WriteOptions
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'YamlFile',
    # Functions
    'read',
    'write',
]


# SECTION: CLASSES ========================================================== #


class YamlFile(SemiStructuredTextFileHandlerABC):
    """
    Handler implementation for YAML files.
    """

    # -- Class Attributes -- #

    format = FileFormat.YAML

    # -- Instance Methods -- #

    def dumps(
        self,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> str:
        """
        Serialize *data* to YAML text.

        Parameters
        ----------
        data : JSONData
            Payload to serialize.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        str
            Serialized YAML text.
        """
        _ = options
        from io import StringIO

        stream = StringIO()
        get_yaml().safe_dump(
            data,
            stream,
            sort_keys=False,
            allow_unicode=True,
            default_flow_style=False,
        )
        return stream.getvalue()

    def loads(
        self,
        text: str,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Parse YAML *text* into structured records.

        Parameters
        ----------
        text : str
            YAML payload as text.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONData
            Parsed payload.
        """
        _ = options
        from io import StringIO

        loaded = get_yaml().safe_load(StringIO(text))
        return coerce_record_payload(loaded, format_name='YAML')


# SECTION: INTERNAL CONSTANTS =============================================== #

_YAML_HANDLER = YamlFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONData:
    """
    Deprecated wrapper. Use ``YamlFile().read(...)`` instead.

    Validates that the YAML root is a dict or a list of dicts.

    Parameters
    ----------
    path : StrPath
        Path to the YAML file on disk.

    Returns
    -------
    JSONData
        The structured data read from the YAML file.
    """
    return call_deprecated_module_read(
        path,
        __name__,
        _YAML_HANDLER.read,
    )


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Deprecated wrapper. Use ``YamlFile().write(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the YAML file on disk.
    data : JSONData
        Data to write as YAML.

    Returns
    -------
    int
        The number of records written.
    """
    return call_deprecated_module_write(
        path,
        data,
        __name__,
        _YAML_HANDLER.write,
    )
