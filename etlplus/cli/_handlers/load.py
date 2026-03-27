"""
:mod:`etlplus.cli._handlers.load` module.

Load-command handler.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any
from typing import cast

from ...ops import load
from .. import _io
from .common import _complete_and_emit_json
from .common import _complete_and_emit_or_write
from .common import _fail_command
from .common import _start_command

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'load_handler',
]


# SECTION: FUNCTIONS ======================================================== #


def load_handler(
    *,
    source: str,
    target_type: str,
    target: str,
    event_format: str | None = None,
    source_format: str | None = None,
    target_format: str | None = None,
    format_explicit: bool = False,
    output: str | None = None,
    pretty: bool = True,
) -> int:
    """
    Load data into a target.

    Parameters
    ----------
    source : str
        Source to load data from.
    target_type : str
        Type of the target.
    target : str
        Target to load data into.
    event_format : str | None, optional
        Format of structured events.
    source_format : str | None, optional
        Format of the source data.
    target_format : str | None, optional
        Format of the target data (if applicable).
    format_explicit : bool, optional
        Whether the format(s) were explicitly specified by the user (as opposed
        to being inferred).
    output : str | None, optional
        Output path to save the load result to (if not using --target).
    pretty : bool, optional
        Whether to pretty-print JSON output.

    Returns
    -------
    int
        Exit code (0 if load succeeded, non-zero if any errors occurred).

    Raises
    ------
    Exception
        If any error occurs during loading.
    """
    explicit_format = target_format if format_explicit else None
    context = _start_command(
        command='load',
        event_format=event_format,
        source=source,
        target=target,
        target_type=target_type,
    )

    try:
        source_value = cast(
            str | Path | os.PathLike[str] | dict[str, Any] | list[dict[str, Any]],
            _io.resolve_cli_payload(
                source,
                format_hint=source_format,
                format_explicit=source_format is not None,
                hydrate_files=False,
            ),
        )

        if target_type == 'file' and target == '-':
            payload = _io.materialize_file_payload(
                source_value,
                format_hint=source_format,
                format_explicit=source_format is not None,
            )
            return _complete_and_emit_json(
                context,
                payload,
                pretty=pretty,
                result_status='ok',
                source=source,
                status='ok',
                target=target,
                target_type=target_type,
            )

        result = load(
            source_value,
            target_type,
            target,
            file_format=explicit_format,
        )

        return _complete_and_emit_or_write(
            context,
            result,
            output,
            pretty=pretty,
            success_message='Load result saved to',
            destination=output or 'stdout',
            result_status=result.get('status') if isinstance(result, dict) else 'ok',
            source=source,
            status='ok',
            target=target,
            target_type=target_type,
        )
    except Exception as exc:
        _fail_command(
            context,
            exc,
            source=source,
            target=target,
            target_type=target_type,
        )
        raise
