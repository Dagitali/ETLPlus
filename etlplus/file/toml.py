"""
:mod:`etlplus.file.toml` module.

Helpers for reading/writing Tom's Obvious Minimal Language (TOML) files.

Notes
-----
- A TOML file is a configuration file that uses the TOML syntax.
- Common cases:
    - Simple key-value pairs.
    - Nested tables and arrays.
    - Data types such as strings, integers, floats, booleans, dates, and
        arrays.
- Rule of thumb:
    - If the file follows the TOML specification, use this module for
        reading and writing.
"""

from __future__ import annotations

import tomllib
from typing import Any

from ..utils._types import JSONDict
from ._enums import FileFormat
from ._imports import get_dependency
from ._semi_structured_handlers import DictPayloadTextCodecHandlerMixin

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'TomlFile',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _tomli_w() -> Any:
    """Return required :mod:`tomli_w` module for TOML writes."""
    return get_dependency(
        'tomli_w',
        format_name='TOML',
        pip_name='tomli-w',
        required=True,
    )


# SECTION: CLASSES ========================================================== #


class TomlFile(DictPayloadTextCodecHandlerMixin):
    """Handler implementation for TOML files."""

    # -- Class Attributes -- #

    format = FileFormat.TOML
    dict_root_error_message = 'TOML root must be a table (dict)'

    # -- Instance Methods -- #

    def decode_dict_payload_text(
        self,
        text: str,
    ) -> object:
        """Parse TOML *text* into a dictionary payload."""
        return tomllib.loads(text)

    def encode_dict_payload_text(
        self,
        payload: JSONDict,
    ) -> str:
        """Serialize dictionary *data* into TOML text."""
        return _tomli_w().dumps(payload)
