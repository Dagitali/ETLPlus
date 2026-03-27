"""
:mod:`etlplus.cli._handlers.validate` module.

Validate-command handler.
"""

from __future__ import annotations

import sys
from collections.abc import Mapping
from typing import cast

from ...ops import validate
from ...ops.validate import FieldRulesDict
from ...utils.types import JSONData
from .. import _io
from .common import _complete_and_emit_json
from .common import _complete_and_write_json_output
from .common import _fail_command
from .common import _start_command

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'validate_handler',
]


# SECTION: FUNCTIONS ======================================================== #


def validate_handler(
    *,
    source: str,
    rules: JSONData | str,
    event_format: str | None = None,
    source_format: str | None = None,
    target: str | None = None,
    format_explicit: bool = False,
    pretty: bool = True,
) -> int:
    """
    Validate data from a source.

    Parameters
    ----------
    source : str
        Source to validate data from.
    rules : JSONData | str
        Validation rules to apply to the data (either as a JSON-serializable
        object or as a path to a file containing the JSON).
    event_format : str | None, optional
        Format of structured events to emit during the validation (if not
        specified, events will be emitted in a default format).
    source_format : str | None, optional
        Format of the source data (if not specified, the format will be
        inferred from the source or from the data itself).
    target : str | None, optional
        Target to save the validation results to (if not specified, results
        will be emitted to stdout).
    format_explicit : bool, optional
        Whether the format(s) were explicitly specified by the user (as opposed
        to being inferred).
    pretty : bool, optional
        Whether to pretty-print JSON output.

    Returns
    -------
    int
        Exit code (0 if validation succeeded, non-zero if any errors occurred).

    Raises
    ------
    Exception
        If any error occurs during validation or loading.
    ValueError
        If the provided rules do not resolve to a mapping of field rules.
    """
    format_hint: str | None = source_format
    context = _start_command(
        command='validate',
        event_format=event_format,
        source=source,
        target=target or 'stdout',
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

        rules_payload = _io.resolve_cli_payload(
            rules,
            format_hint=None,
            format_explicit=format_explicit,
        )
        if not isinstance(rules_payload, dict):
            raise ValueError('rules must resolve to a mapping of field rules')

        field_rules = cast(Mapping[str, FieldRulesDict], rules_payload)
        result = validate(payload, field_rules)

        if target and target != '-':
            validated_data = result.get('data')
            if validated_data is not None:
                return _complete_and_write_json_output(
                    context,
                    validated_data,
                    target,
                    success_message='ValidationDict result saved to',
                    result_status='ok',
                    source=source,
                    status='ok',
                    target=target,
                    valid=result.get('valid'),
                )
            print(
                f'ValidationDict failed, no data to save for {target}',
                file=sys.stderr,
            )
            return 0

        return _complete_and_emit_json(
            context,
            result,
            pretty=pretty,
            result_status='ok',
            source=source,
            status='ok',
            target=target or 'stdout',
            valid=result.get('valid'),
        )
    except Exception as exc:
        _fail_command(
            context,
            exc,
            source=source,
            target=target or 'stdout',
        )
        raise
