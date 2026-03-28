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

from io import StringIO
from typing import Any

from ..utils.types import JSONData
from ._enums import FileFormat
from ._imports import get_yaml
from ._semi_structured_handlers import RecordPayloadTextCodecHandlerMixin

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'YamlFile',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _yaml() -> Any:
    """Return the required PyYAML module."""
    return get_yaml()


# SECTION: CLASSES ========================================================== #


class YamlFile(RecordPayloadTextCodecHandlerMixin):
    """Handler implementation for YAML files."""

    # -- Class Attributes -- #

    format = FileFormat.YAML

    # -- Instance Methods -- #

    def decode_text_payload(
        self,
        text: str,
    ) -> object:
        """Parse YAML *text* into a Python payload."""
        yaml = _yaml()
        return yaml.safe_load(StringIO(text))

    def encode_text_payload(
        self,
        data: JSONData,
    ) -> str:
        """Serialize *data* to YAML text."""
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
