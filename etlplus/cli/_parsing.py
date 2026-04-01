"""
:mod:`etlplus.cli._parsing` module.

Shared payload-parsing helpers for CLI commands and handlers.
"""

from __future__ import annotations

import json
from typing import cast

from ..utils._types import JSONData

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'parse_json_payload',
]


# SECTION: FUNCTIONS ======================================================== #


def parse_json_payload(text: str) -> JSONData:
    """
    Parse JSON text and surface a concise error when it fails.

    Parameters
    ----------
    text : str
        The JSON text to parse.

    Returns
    -------
    JSONData
        The parsed JSON data.

    Raises
    ------
    ValueError
        When the JSON text is invalid.
    """
    try:
        return cast(JSONData, json.loads(text))
    except json.JSONDecodeError as e:
        raise ValueError(
            f'Invalid JSON payload: {e.msg} (pos {e.pos})',
        ) from e
