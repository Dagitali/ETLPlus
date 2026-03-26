"""
:mod:`etlplus.cli._commands.helpers` module.

Shared helper functions for CLI command modules.
"""

from __future__ import annotations

from typing import Any

import typer

from etlplus.cli._io import parse_json_payload

# SECTION: EXPORTS ========================================================== #


__all__ = ['_parse_json_option']


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _parse_json_option(
    value: str,
    flag: str,
) -> Any:
    """
    Parse JSON option values and surface a helpful CLI error.

    Parameters
    ----------
    value : str
        The JSON string to parse.
    flag : str
        The CLI flag name for error messages.

    Returns
    -------
    Any
        The parsed JSON value.

    Raises
    ------
    typer.BadParameter
        When the JSON is invalid.
    """
    try:
        return parse_json_payload(value)
    except ValueError as exc:
        raise typer.BadParameter(f'Invalid JSON for {flag}: {exc}') from exc
