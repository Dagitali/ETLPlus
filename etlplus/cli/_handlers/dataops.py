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
from .. import _io
from . import lifecycle as _lifecycle
from . import output as _output
from . import payload as _payload

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'extract_handler',
    'load_handler',
    'transform_handler',
    'validate_handler',
]


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
    return _output.complete_output(
        context,
        payload,
        mode=mode,
        pretty=pretty,
        result_status=result_status,
        status='ok',
        **fields,
    )


def _resolve_source_mapping_inputs(
    *,
    source: str,
    mapping_payload: JSONData | str,
    source_format: str | None,
    format_explicit: bool,
    error_message: str,
) -> tuple[JSONData | str, dict[str, Any]]:
    """Resolve a source payload plus a required mapping-style side payload."""
    source_format_explicit = source_format is not None or format_explicit
    payload = cast(
        JSONData | str,
        _payload.resolve_payload(
            source,
            format_hint=source_format,
            format_explicit=source_format_explicit,
        ),
    )
    mapping = _payload.resolve_mapping_payload(
        mapping_payload,
        format_explicit=source_format_explicit,
        error_message=error_message,
    )
    return payload, mapping


# SECTION: FUNCTIONS ======================================================== #


def extract_handler(
    *,
    source_type: str,
    source: str,
    event_format: str | None = None,
    format_hint: str | None = None,
    format_explicit: bool = False,
    target: str | None = None,
    output: str | None = None,
    pretty: bool = True,
) -> int:
    """
    Extract data from a source.

    Parameters
    ----------
    source_type : str
        The type of the source (e.g., "file", "database", or "api").
    source : str
        The source location (e.g., a file path, a database connection string,
        or an API endpoint).
    event_format : str | None, optional
        The requested event output format (e.g., "jsonl" or ``None`` for no
        events).
    format_hint : str | None, optional
        An optional format hint for the source data  to assist with parsing
        when the format is not explicit.
    format_explicit : bool, optional
        Whether the format hint is explicit (e.g., via a CLI option) and should
        be used as-is without inference. Default is ``False``.
    target : str | None, optional
        The target location for the extracted data. Default is ``None``.
    output : str | None, optional
        The output location for the extracted data. Default is ``None``.
    pretty : bool, optional
        Whether to pretty-print the output. Default is ``True``.
    Returns
    -------
    int
        The CLI exit code.
    """
    explicit_format = format_hint if format_explicit else None
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
            payload = _io.parse_text_payload(_io.read_stdin_text(), format_hint)
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
        The source location (e.g., a file path, a database connection string,
        or an API endpoint).
    target_type : str
        The type of the target (e.g., "file", "database", or "api").
    target : str
        The target location (e.g., a file path, a database connection string,
        or an API endpoint).
    event_format : str | None, optional
        The requested event output format (e.g., "jsonl" or ``None`` for no
        events).
    source_format : str | None, optional
        An optional format hint for the source data to assist with parsing
        when the format is not explicit.
    target_format : str | None, optional
        An optional format hint for the target data to assist with parsing
        when the format is not explicit.
    format_explicit : bool, optional
        Whether the format hint is explicit (e.g., via a CLI option) and should
        be used as-is without inference. Default is ``False``.
    output : str | None, optional
        The output location for the loaded data. Default is ``None``.
    pretty : bool, optional
        Whether to pretty-print the output. Default is ``True``.

    Returns
    -------
    int
        The CLI exit code.
    """
    source_format_explicit = source_format is not None
    target_format_explicit = target_format is not None or format_explicit
    command_fields: dict[str, Any] = {
        'source': source,
        'target': target,
        'target_type': target_type,
    }

    with _command_scope(
        command='load',
        event_format=event_format,
        fields=command_fields,
    ) as context:
        source_value = cast(
            str | JSONData,
            _payload.resolve_payload(
                source,
                format_hint=source_format,
                format_explicit=source_format_explicit,
                hydrate_files=False,
            ),
        )

        if target_type == 'file' and target == '-':
            return _complete_success(
                context,
                _io.materialize_file_payload(
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
        result_status = (
            cast(str, result.get('status') or 'ok')
            if isinstance(result, dict)
            else 'ok'
        )

        return _complete_success(
            context,
            result,
            mode='or_write',
            output_path=output,
            pretty=pretty,
            success_message='Load result saved to',
            destination=output or 'stdout',
            result_status=result_status,
            **command_fields,
        )


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
    Transform data from a source and optionally write the result.

    Parameters
    ----------
    source : str
        The source location (e.g., a file path, a database connection string,
        or an API endpoint).
    operations : JSONData | str
        The operations to apply to the source data.
    target : str | None, optional
        The target location for the transformed data. Default is ``None``.
    target_type : str | None, optional
        The type of the target (e.g., "file", "database", or "api").
    event_format : str | None, optional
        The requested event output format (e.g., "jsonl" or ``None`` for no
        events).
    source_format : str | None, optional
        An optional format hint for the source data to assist with parsing
        when the format is not explicit.
    target_format : str | None, optional
        An optional format hint for the target data to assist with parsing
        when the format is not explicit.
    pretty : bool, optional
        Whether to pretty-print the output. Default is ``True``.
    format_explicit : bool, optional
        Whether the format hints are explicit (e.g., via CLI options) and
        should be used as-is without inference. Default is ``False``.

    Returns
    -------
    int
        The CLI exit code.
    """
    target_format_explicit = target_format is not None or format_explicit
    target_label = target or 'stdout'
    command_fields: dict[str, Any] = {
        'source': source,
        'target': target_label,
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
        The source location (e.g., a file path, a database connection string,
        or an API endpoint).
    rules : JSONData | str
        The validation rules to apply to the source data.
    event_format : str | None, optional
        The requested event output format (e.g., "jsonl" or ``None`` for no
        events).
    source_format : str | None, optional
        An optional format hint for the source data to assist with parsing
        when the format is not explicit.
    target : str | None, optional
        The target location for the validation results. Default is ``None``.
    format_explicit : bool, optional
        Whether the format hints are explicit (e.g., via CLI options) and
        should be used as-is without inference. Default is ``False``.
    pretty : bool, optional
        Whether to pretty-print the output. Default is ``True``.

    Returns
    -------
    int
        The CLI exit code.
    """
    target_label = target or 'stdout'
    command_fields: dict[str, Any] = {
        'source': source,
        'target': target_label,
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
