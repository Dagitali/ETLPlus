"""
:mod:`etlplus.cli._handlers.transform` module.

Transform-command handler.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from typing import Literal
from typing import cast

from ...ops import load
from ...ops import transform
from ...utils.types import JSONData
from .. import _io
from .common import _complete_and_emit_json
from .common import _complete_and_write_file_payload
from .common import _fail_command
from .common import _start_command

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'transform_handler',
]


# SECTION: TYPE ALIASES ===================================================== #


type TransformOperations = Mapping[
    Literal['filter', 'map', 'select', 'sort', 'aggregate'],
    Any,
]


# SECTION: FUNCTIONS ======================================================== #


def transform_handler(
    *,
    source: str,
    operations: JSONData | str,
    target: str | None = None,
    target_type: str | None = None,
    event_format: str | None = None,
    source_format: str | None = None,
    target_format: str | None = None,
    pretty: bool = True,
    format_explicit: bool = False,
) -> int:
    """
    Transform data from a source.

    Parameters
    ----------
    source : str
        Source to transform data from.
    operations : JSONData | str
        Transform operations to apply to the data (either as a JSON-serializable
        object or as a path to a file containing the JSON).
    target : str | None, optional
        Target to save the transformed data to (if not specified, transformed data
        will be emitted to stdout).
    target_type : str | None, optional
        Type of the target (if not specified, defaults to 'file' if *target* is
        specified, ``stdout`` if not).
    event_format : str | None, optional
        Format of structured events to emit during the transformation (if not
        specified, events will be emitted in a default format).
    source_format : str | None, optional
        Format of the source data (if not specified, the format will be
        inferred from the source or from the data itself).
    target_format : str | None, optional
        Format of the target data (if applicable; if not specified, the format
        will be inferred from the target or from the data itself).
    pretty : bool, optional
        Whether to pretty-print JSON output.
    format_explicit : bool, optional
        Whether the format(s) were explicitly specified by the user (as opposed
        to being inferred).

    Returns
    -------
    int
        Exit code (0 if transformation succeeded, non-zero if any errors
        occurred).

    Raises
    ------
    Exception
        If any error occurs during transformation or loading.
    ValueError
        If the provided operations are invalid (e.g. if *operations* does not
        resolve to a mapping of transform operations).
    """
    format_hint: str | None = source_format
    format_explicit = format_hint is not None or format_explicit
    context = _start_command(
        command='transform',
        event_format=event_format,
        source=source,
        target=target or 'stdout',
        target_type=target_type,
    )

    try:
        payload = cast(
            JSONData | str,
            _io.resolve_cli_payload(
                source,
                format_hint=format_hint,
                format_explicit=format_explicit,
            ),
        )

        operations_payload = _io.resolve_cli_payload(
            operations,
            format_hint=None,
            format_explicit=format_explicit,
        )
        if not isinstance(operations_payload, dict):
            raise ValueError('operations must resolve to a mapping of transforms')

        data = transform(payload, cast(TransformOperations, operations_payload))

        if target and target != '-':
            if target_type not in (None, 'file'):
                resolved_target_type = cast(str, target_type)
                result = load(
                    data,
                    resolved_target_type,
                    target,
                    file_format=target_format if format_explicit else None,
                )
                return _complete_and_emit_json(
                    context,
                    result,
                    pretty=pretty,
                    result_status='ok',
                    source=source,
                    status='ok',
                    target=target,
                    target_type=resolved_target_type,
                )

            return _complete_and_write_file_payload(
                context,
                data,
                target,
                format_hint=target_format,
                success_message='Data transformed and saved to',
                result_status='ok',
                source=source,
                status='ok',
                target=target,
                target_type=target_type or 'file',
            )

        return _complete_and_emit_json(
            context,
            data,
            pretty=pretty,
            result_status='ok',
            source=source,
            status='ok',
            target=target or 'stdout',
            target_type=target_type,
        )
    except Exception as exc:
        _fail_command(
            context,
            exc,
            source=source,
            target=target or 'stdout',
            target_type=target_type,
        )
        raise
