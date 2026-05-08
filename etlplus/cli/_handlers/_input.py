"""
:mod:`etlplus.cli._handlers._input` module.

CLI payload parsing and input materialization helpers shared by CLI handler
entrypoints.
"""

from __future__ import annotations

import sys

from ...utils._payloads import infer_payload_format
from ...utils._payloads import materialize_file_payload
from ...utils._payloads import parse_text_payload
from ...utils._payloads import read_csv_rows
from ...utils._types import JSONData

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'infer_payload_format',
    'is_stdin_source',
    'materialize_file_payload',
    'parse_text_payload',
    'read_csv_rows',
    'read_stdin_text',
    'resolve_cli_payload',
]


# SECTION: FUNCTIONS ======================================================== #


def is_stdin_source(
    source: object,
) -> bool:
    """
    Return whether a CLI source represents STDIN.

    Parameters
    ----------
    source : object
        Source value supplied by the CLI.

    Returns
    -------
    bool
        ``True`` for ``"-"`` with optional surrounding whitespace.
    """
    return isinstance(source, str) and source.strip() == '-'


def read_stdin_text() -> str:
    """Return entire STDIN payload."""
    return sys.stdin.read()


def resolve_cli_payload(
    source: object,
    *,
    format_hint: str | None,
    format_explicit: bool,
    hydrate_files: bool = True,
) -> JSONData | object:
    """
    Normalize CLI-provided payloads, honoring STDIN and inline data.

    Parameters
    ----------
    source : object
        The source payload, potentially STDIN or a file path.
    format_hint : str | None
        An optional format hint (e.g., 'json', 'csv').
    format_explicit : bool
        Whether the format hint was explicitly provided.
    hydrate_files : bool, optional
        Whether to materialize file-based payloads. Default is True.

    Returns
    -------
    JSONData | object
        The resolved payload.
    """
    if is_stdin_source(source):
        text = read_stdin_text()
        return parse_text_payload(text, format_hint)

    if not hydrate_files:
        return source

    return materialize_file_payload(
        source,
        format_hint=format_hint,
        format_explicit=format_explicit,
    )
