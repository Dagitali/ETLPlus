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

from typing import Any

from ..types import JSONData
from ._imports import get_yaml
from .base import ReadOptions
from .base import RecordPayloadSemiStructuredTextFileHandlerABC
from .base import WriteOptions
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'YamlFile',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _yaml() -> Any:
    """Return the optional PyYAML module."""
    return get_yaml()


# SECTION: CLASSES ========================================================== #


class YamlFile(RecordPayloadSemiStructuredTextFileHandlerABC):
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
        yaml = _yaml()
        yaml.safe_dump(
            data,
            stream,
            sort_keys=False,
            allow_unicode=True,
            default_flow_style=False,
        )
        return stream.getvalue()

    def loads_payload(
        self,
        text: str,
        *,
        options: ReadOptions | None = None,
    ) -> object:
        """
        Parse YAML *text* into a Python payload.

        Parameters
        ----------
        text : str
            YAML payload as text.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        object
            Parsed payload.
        """
        _ = options
        from io import StringIO

        yaml = _yaml()
        return yaml.safe_load(StringIO(text))
