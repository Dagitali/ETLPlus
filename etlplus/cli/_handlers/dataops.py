"""
:mod:`etlplus.cli._handlers.dataops` module.

Data-operation handler implementations for the CLI facade.
"""

from __future__ import annotations

import sys
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any
from typing import cast

from ...ops import extract
from ...ops import load
from ...ops import transform
from ...ops import validate
from ...ops._types import PipelineConfig
from ...ops.validate import FieldRulesDict
from ...utils._types import JSONData
from . import _completion
from . import _input
from . import _lifecycle
from . import _payload

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'extract_handler',
    'load_handler',
    'transform_handler',
    'validate_handler',
]


# SECTION: TYPE ALIASES ===================================================== #


type _ResolvedSourcePayload = JSONData | str


# SECTION: INTERNAL FUNCTIONS =============================================== #


@contextmanager
def _command_scope(
    *,
    command: str,
    event_format: str | None,
    fields: dict[str, Any],
) -> Iterator[_lifecycle.CommandContext]:
    """Start a command context and wrap it in the shared failure boundary."""
    context = _lifecycle.start_command(
        command=command,
        event_format=event_format,
        **fields,
    )
    try:
        yield context
    except Exception as exc:
        _lifecycle.fail_command(context, exc, **fields)
        raise


def _complete_success(
    context: _lifecycle.CommandContext,
    payload: Any,
    *,
    mode: str,
    pretty: bool = True,
    result_status: str = 'ok',
    **fields: Any,
) -> int:
    """Complete a command using the shared successful-status fields."""
    return _completion.complete_output(
        context,
        payload,
        mode=mode,
        pretty=pretty,
        result_status=result_status,
        status='ok',
        **fields,
    )


def _display_target(
    target: str | None,
) -> str:
    """Return a human-readable target label for lifecycle events."""
    if target in (None, '-'):
        return 'stdout'
    assert target is not None
    return target


def _is_explicit_format(
    *,
    format_hint: str | None,
    explicit: bool,
) -> bool:
    """Return True when a format hint should be treated as explicit."""
    return format_hint is not None or explicit


def _resolve_source_mapping_inputs(
    *,
    source: str,
    mapping_payload: JSONData | str,
    source_format: str | None,
    format_explicit: bool,
    error_message: str,
) -> tuple[JSONData | str, dict[str, Any]]:
    """Resolve a source payload plus a required mapping-style side payload."""
    source_format_explicit = _is_explicit_format(
        format_hint=source_format,
        explicit=format_explicit,
    )
    payload = _resolve_source_payload(
        source,
        source_format=source_format,
        format_explicit=source_format_explicit,
    )
    mapping = _payload.resolve_mapping_payload(
        mapping_payload,
        format_explicit=source_format_explicit,
        error_message=error_message,
    )
    return payload, mapping


def _resolve_source_payload(
    source: str,
    *,
    source_format: str | None,
    format_explicit: bool,
    hydrate_files: bool = True,
) -> _ResolvedSourcePayload:
    """Resolve one CLI source argument into a loadable payload."""
    return cast(
        _ResolvedSourcePayload,
        _payload.resolve_payload(
            source,
            format_hint=source_format,
            format_explicit=format_explicit,
            hydrate_files=hydrate_files,
        ),
    )


def _result_status(
    result: object,
    *,
    default: str = 'ok',
) -> str:
    """Extract a string status field from one result payload."""
    if not isinstance(result, dict):
        return default
    status = result.get('status')
    return cast(str, status) if isinstance(status, str) else default


# SECTION: FUNCTIONS ======================================================== #


def extract_handler(
    *,
    source: str,
    source_type: str,
    target: str | None = None,
    source_format: str | None = None,
    format_explicit: bool = False,
    event_format: str | None = None,
    output: str | None = None,
    pretty: bool = True,
) -> int:
    """
    Extract data from a source.

    Parameters
    ----------
    source : str
        Source path, URI/URL, connector reference, or ``-`` for STDIN.
    source_type : str
        Source connector type, such as ``file``, ``database``, or ``api``.
    target : str | None, optional
        Optional target location for the extracted data. Default is ``None``.
    source_format : str | None, optional
        Optional format hint for the source payload. Default is ``None``.
    format_explicit : bool, optional
        Whether *source_format* was explicitly provided and should not be
        inferred. Default is ``False``.
    event_format : str | None, optional
        Structured event output format. Default is ``None``.
    output : str | None, optional
        Optional path to write the extracted payload. Default is ``None``.
    pretty : bool, optional
        Whether to pretty-print JSON output. Default is ``True``.

    Returns
    -------
    int
        CLI exit code indicating success (``0``) or failure (non-zero).
    """
    explicit_format = source_format if format_explicit else None
    command_fields: dict[str, Any] = {
        'source': source,
        'source_type': source_type,
    }

    with _command_scope(
        command='extract',
        event_format=event_format,
        fields=command_fields,
    ) as context:
        if source == '-':
            payload = _input.parse_text_payload(
                _input.read_stdin_text(),
                source_format,
            )
            return _complete_success(
                context,
                payload,
                mode='json',
                pretty=pretty,
                **command_fields,
            )

        output_path = target or output
        return _complete_success(
            context,
            extract(
                source_type,
                source,
                file_format=explicit_format,
            ),
            mode='or_write',
            output_path=output_path,
            pretty=pretty,
            success_message='Data extracted and saved to',
            destination=output_path or 'stdout',
            **command_fields,
        )


def load_handler(
    *,
    source: str,
    target_type: str,
    target: str,
    source_format: str | None = None,
    target_format: str | None = None,
    format_explicit: bool = False,
    event_format: str | None = None,
    output: str | None = None,
    pretty: bool = True,
) -> int:
    """
    Load data into a target.

    Parameters
    ----------
    source : str
        Source path, URI/URL, connector reference, or ``-`` for STDIN.
    target_type : str
        Target connector type such as ``file``, ``database``, or ``api``.
    target : str
        Target path, URI/URL, connector reference, or ``-`` for STDOUT.
    source_format : str | None, optional
        Optional format hint for the source payload. Default is ``None``.
    target_format : str | None, optional
        Optional format hint for the target payload. Default is ``None``.
    format_explicit : bool, optional
        Whether *target_format* was explicitly provided and should not be
        inferred. Default is ``False``.
    event_format : str | None, optional
        Structured event output format. Default is ``None``.
    output : str | None, optional
        Optional path to write the load result. Default is ``None``.
    pretty : bool, optional
        Whether to pretty-print JSON output. Default is ``True``.

    Returns
    -------
    int
        Exit code indicating success (``0``) or failure (non-zero).
    """
    source_format_explicit = _is_explicit_format(
        format_hint=source_format,
        explicit=False,
    )
    target_format_explicit = _is_explicit_format(
        format_hint=target_format,
        explicit=format_explicit,
    )
    command_fields: dict[str, Any] = {
        'source': source,
        'target': _display_target(target),
        'target_type': target_type,
    }

    with _command_scope(
        command='load',
        event_format=event_format,
        fields=command_fields,
    ) as context:
        source_value = _resolve_source_payload(
            source,
            source_format=source_format,
            format_explicit=source_format_explicit,
            hydrate_files=False,
        )

        if target_type == 'file' and target == '-':
            return _complete_success(
                context,
                _input.materialize_file_payload(
                    source_value,
                    format_hint=source_format,
                    format_explicit=source_format_explicit,
                ),
                mode='json',
                pretty=pretty,
                **command_fields,
            )

        result = load(
            source_value,
            target_type,
            target,
            file_format=target_format if target_format_explicit else None,
        )

        return _complete_success(
            context,
            result,
            mode='or_write',
            output_path=output,
            pretty=pretty,
            success_message='Load result saved to',
            destination=output or 'stdout',
            result_status=_result_status(result),
            **command_fields,
        )


def transform_handler(
    *,
    source: str,
    operations: JSONData | str,
    target: str | None = None,
    target_type: str | None = None,
    source_format: str | None = None,
    target_format: str | None = None,
    format_explicit: bool = False,
    event_format: str | None = None,
    pretty: bool = True,
) -> int:
    """
    Transform data from a source and optionally write the result.

    Parameters
    ----------
    source : str
        Source path, URI/URL, connector reference, or ``-`` for STDIN.
    operations : JSONData | str
        Transformation operations to apply to the source payload.
    target : str | None, optional
        Optional destination for the transformed data. Default is ``None``.
    target_type : str | None, optional
        Optional destination connector type. Default is ``None``.
    source_format : str | None, optional
        Optional format hint for the source payload. Default is ``None``.
    target_format : str | None, optional
        Optional format hint for the destination payload. Default is ``None``.
    format_explicit : bool, optional
        Whether the provided format hints were explicitly supplied and should
        not be inferred. Default is ``False``.
    event_format : str | None, optional
        Structured event output format. Default is ``None``.
    pretty : bool, optional
        Whether to pretty-print JSON output. Default is ``True``.

    Returns
    -------
    int
        CLI exit code indicating success (``0``) or failure (non-zero).
    """
    target_format_explicit = target_format is not None or format_explicit
    command_fields: dict[str, Any] = {
        'source': source,
        'target': _display_target(target),
        'target_type': target_type,
    }

    with _command_scope(
        command='transform',
        event_format=event_format,
        fields=command_fields,
    ) as context:
        payload, operations_payload = _resolve_source_mapping_inputs(
            source=source,
            mapping_payload=operations,
            source_format=source_format,
            format_explicit=format_explicit,
            error_message='operations must resolve to a mapping of transforms',
        )
        data = transform(payload, cast(PipelineConfig, operations_payload))

        if target and target != '-':
            if target_type not in (None, 'file'):
                resolved_target_type = cast(str, target_type)
                return _complete_success(
                    context,
                    load(
                        data,
                        resolved_target_type,
                        target,
                        file_format=target_format if target_format_explicit else None,
                    ),
                    mode='json',
                    pretty=pretty,
                    source=source,
                    target=target,
                    target_type=resolved_target_type,
                )

            return _complete_success(
                context,
                data,
                mode='file',
                output_path=target,
                format_hint=target_format,
                success_message='Data transformed and saved to',
                source=source,
                target=target,
                target_type=target_type or 'file',
            )

        return _complete_success(
            context,
            data,
            mode='json',
            pretty=pretty,
            **command_fields,
        )


def validate_handler(
    *,
    source: str,
    rules: JSONData | str,
    target: str | None = None,
    source_format: str | None = None,
    format_explicit: bool = False,
    event_format: str | None = None,
    pretty: bool = True,
) -> int:
    """
    Validate data from a source.

    Parameters
    ----------
    source : str
        Source path, URI/URL, connector reference, or ``-`` for STDIN.
    rules : JSONData | str
        Validation rules to apply to the source payload.
    target : str | None, optional
        Optional destination for validated output. Default is ``None``.
    source_format : str | None, optional
        Optional format hint for the source payload. Default is ``None``.
    format_explicit : bool, optional
        Whether *source_format* was explicitly provided and should not be
        inferred. Default is ``False``.
    event_format : str | None, optional
        Structured event output format. Default is ``None``.
    pretty : bool, optional
        Whether to pretty-print JSON output. Default is ``True``.

    Returns
    -------
    int
        CLI exit code indicating success (``0``) or failure (non-zero).
    """
    command_fields: dict[str, Any] = {
        'source': source,
        'target': _display_target(target),
    }

    with _command_scope(
        command='validate',
        event_format=event_format,
        fields=command_fields,
    ) as context:
        payload, rules_payload = _resolve_source_mapping_inputs(
            source=source,
            mapping_payload=rules,
            source_format=source_format,
            format_explicit=format_explicit,
            error_message='rules must resolve to a mapping of field rules',
        )
        result = validate(
            payload,
            cast(dict[str, FieldRulesDict], rules_payload),
        )

        if target and target != '-':
            validated_data = result.get('data')
            if validated_data is not None:
                return _complete_success(
                    context,
                    validated_data,
                    mode='json_file',
                    output_path=target,
                    success_message='ValidationDict result saved to',
                    source=source,
                    target=target,
                    valid=result.get('valid'),
                )

            print(
                f'ValidationDict failed, no data to save for {target}',
                file=sys.stderr,
            )
            return 0

        return _complete_success(
            context,
            result,
            mode='json',
            pretty=pretty,
            valid=result.get('valid'),
            **command_fields,
        )
