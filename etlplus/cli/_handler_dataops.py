"""
:mod:`etlplus.cli._handler_dataops` module.

Data-operation handler implementations for the CLI facade.
"""

from __future__ import annotations

import sys
from typing import Any
from typing import cast

from ..ops.validate import FieldRulesDict
from ..utils._types import JSONData
from . import _handler_common as _common_impl
from . import _io

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'extract_handler',
    'load_handler',
    'transform_handler',
    'validate_handler',
]


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
    extract_fn: Any,
    start_command: Any,
    failure_boundary: Any,
    complete_output: Any,
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
    extract_fn : Any
        The callable to use for extracting data, which should accept parameters
        (source_type, source, file_format) and return the extracted data.
    start_command : Any
        The callable to start the command context for logging and telemetry,
        expected to accept parameters (command, event_format, source,
        source_type) and return a context object.
    failure_boundary : Any
        The callable context manager for handling exceptions and logging
        failures, expected to accept parameters (context, source, source_type)
        and yield a context for the command execution block.
    complete_output : Any
        The callable to complete the command with output, expected to accept
        parameters (context, payload, mode, pretty, result_status, source,
        status, target, target_type) and return an exit code.

    Returns
    -------
    int
        The CLI exit code.
    """
    explicit_format = format_hint if format_explicit else None
    context = start_command(
        command='extract',
        event_format=event_format,
        source=source,
        source_type=source_type,
    )

    with failure_boundary(
        context,
        source=source,
        source_type=source_type,
    ):
        if source == '-':
            payload = _io.parse_text_payload(_io.read_stdin_text(), format_hint)
            return complete_output(
                context,
                payload,
                mode='json',
                pretty=pretty,
                result_status='ok',
                status='ok',
                source=source,
                source_type=source_type,
            )

        output_path = target or output
        return complete_output(
            context,
            extract_fn(
                source_type,
                source,
                file_format=explicit_format,
            ),
            mode='or_write',
            output_path=output_path,
            pretty=pretty,
            success_message='Data extracted and saved to',
            destination=output_path or 'stdout',
            result_status='ok',
            source=source,
            source_type=source_type,
            status='ok',
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
    load_fn: Any,
    start_command: Any,
    failure_boundary: Any,
    complete_output: Any,
) -> int:
    """
    Load data into a target.

    Parameters
    ----------
    source : str
        The source location (e.g. a file path or a database connection string).
    target_type : str
        The type of the target (e.g. "file", "database", or "api").
    target : str
        The target location (e.g. a file path or a database connection string).
    event_format : str | None, optional
        The requested event output format for event logging (e.g. "jsonl" or
        ``None`` for no events).
    source_format : str | None, optional
        An optional format hint for the source data to assist with parsing when
        the format is not explicit.
    target_format : str | None, optional
        An optional format hint for the target data to assist with formatting
        when the format is not explicit.
    format_explicit : bool, optional
        Whether the format hints are explicit (e.g. via CLI options) and should
        be used as-is without inference. Default is ``False``.
    output : str | None, optional
        The output location for the load result, if applicable. Default is
        ``None``.
    pretty : bool, optional
        Whether to pretty-print the output. Default is ``True``.
    load_fn : Any
        The function to use for loading data, which should accept parameters
        (source_value, target_type, target, file_format) and return a result
        dict or value.
    start_command : Any
        The function to call to start the command context for logging and
        telemetry, expected to accept parameters (command, event_format,
        source, target, target_type) and return a context object.
    failure_boundary : Any
        The context manager function to use for handling exceptions and logging
        failures, expected to accept parameters (context, source, target,
        target_type) and yield a context for the command execution block.
    complete_output : Any
        The function to call to complete the command execution and produce the
        final output, expected to accept parameters (context, result, mode,
        output_path, pretty, success_message, destination, result_status,
        source, status, target, target_type) and return an integer exit code.

    Returns
    -------
    int
        The CLI exit code.
    """
    source_format_explicit = source_format is not None
    target_format_explicit = target_format is not None or format_explicit
    context = start_command(
        command='load',
        event_format=event_format,
        source=source,
        target=target,
        target_type=target_type,
    )

    with failure_boundary(
        context,
        source=source,
        target=target,
        target_type=target_type,
    ):
        source_value = _common_impl.resolve_payload(
            source,
            format_hint=source_format,
            format_explicit=source_format_explicit,
            hydrate_files=False,
        )

        if target_type == 'file' and target == '-':
            return complete_output(
                context,
                _io.materialize_file_payload(
                    source_value,
                    format_hint=source_format,
                    format_explicit=source_format_explicit,
                ),
                mode='json',
                pretty=pretty,
                result_status='ok',
                source=source,
                status='ok',
                target=target,
                target_type=target_type,
            )

        result = load_fn(
            source_value,
            target_type,
            target,
            file_format=target_format if target_format_explicit else None,
        )
        result_status = result.get('status') if isinstance(result, dict) else 'ok'

        return complete_output(
            context,
            result,
            mode='or_write',
            output_path=output,
            pretty=pretty,
            success_message='Load result saved to',
            destination=output or 'stdout',
            result_status=result_status,
            source=source,
            status='ok',
            target=target,
            target_type=target_type,
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
    load_fn: Any,
    transform_fn: Any,
    start_command: Any,
    failure_boundary: Any,
    complete_output: Any,
) -> int:
    """
    Transform data from a source and optionally write the result.

    Parameters
    ----------
    source : str
        The source location (e.g., a file path or a database connection string)
        for the data to transform or a JSON representation of the data to
        transform.
    operations : JSONData | str
        The transformation operations to apply, either as a JSON-like object or
        a string that resolves to such an object (e.g., a file path).
    target : str | None, optional
        The target location for the transformed data. Default is ``None``.
    target_type : str | None, optional
        The type of the target (e.g., "file", "database", or "api"). Default is
        ``None``.
    event_format : str | None, optional
        The requested event output format for event logging (e.g., "jsonl" or
        ``None`` for no events).
    source_format : str | None, optional
        An optional format hint for the source data to assist with parsing when
        the format is not explicit.
    target_format : str | None, optional
        An optional format hint for the target data to assist with formatting
        when the format is not explicit.
    pretty : bool, optional
        Whether to pretty-print the output. Default is ``True``.
    format_explicit : bool, optional
        Whether the format hints are explicit (e.g., via CLI options) and
        should be used as-is without inference. Default is ``False``.
    load_fn : Any
        The function to use for loading the transformed data, which should
        accept parameters (data, target type, target, file_format) and return a
        result dict or value.
    transform_fn : Any
        The function to use for transforming the data, which should accept
        parameters (payload, operations) and return the transformed data.
    start_command : Any
        The function to call to start the command context for logging and
        telemetry, expected to accept parameters (command, event_format,
        source, target, target_type) and return a context object.
    failure_boundary : Any
        The context manager function to use for handling exceptions and logging
        failures, expected to accept parameters (context, source, target,
        target_type) and yield a context for the command execution block.
    complete_output : Any
        The function to call to complete the command execution and produce the
        final output, expected to accept parameters (context, result, mode,
        output_path, pretty, success_message, destination, result_status,
        source, status, target, target_type) and return an integer exit code.

    Returns
    -------
    int
        The CLI exit code.
    """
    source_format_explicit = source_format is not None or format_explicit
    target_format_explicit = target_format is not None or format_explicit
    target_label = target or 'stdout'
    context = start_command(
        command='transform',
        event_format=event_format,
        source=source,
        target=target_label,
        target_type=target_type,
    )

    with failure_boundary(
        context,
        source=source,
        target=target_label,
        target_type=target_type,
    ):
        payload = cast(
            JSONData | str,
            _common_impl.resolve_payload(
                source,
                format_hint=source_format,
                format_explicit=source_format_explicit,
            ),
        )
        operations_payload = _common_impl.resolve_mapping_payload(
            operations,
            format_explicit=source_format_explicit,
            error_message='operations must resolve to a mapping of transforms',
        )
        data = transform_fn(payload, operations_payload)

        if target and target != '-':
            if target_type not in (None, 'file'):
                resolved_target_type = cast(str, target_type)
                return complete_output(
                    context,
                    load_fn(
                        data,
                        resolved_target_type,
                        target,
                        file_format=(target_format if target_format_explicit else None),
                    ),
                    mode='json',
                    pretty=pretty,
                    result_status='ok',
                    source=source,
                    status='ok',
                    target=target,
                    target_type=resolved_target_type,
                )

            return complete_output(
                context,
                data,
                mode='file',
                output_path=target,
                format_hint=target_format,
                success_message='Data transformed and saved to',
                result_status='ok',
                source=source,
                status='ok',
                target=target,
                target_type=target_type or 'file',
            )

        return complete_output(
            context,
            data,
            mode='json',
            pretty=pretty,
            result_status='ok',
            source=source,
            status='ok',
            target=target_label,
            target_type=target_type,
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
    validate_fn: Any,
    start_command: Any,
    failure_boundary: Any,
    complete_output: Any,
    print_fn: Any = print,
    stderr: Any = sys.stderr,
) -> int:
    """
    Validate data from a source.

    Parameters
    ----------
    source : str
        The source location (e.g., a file path or a database connection string)
        for the data to validate or a JSON representation of the data to
        validate.
    rules : JSONData | str
        The validation rules to apply to the data.
    event_format : str | None, optional
        The format of the events, by default ``None``.
    source_format : str | None, optional
        The format of the source data, by default ``None``.
    target : str | None, optional
        The target to write the validation results to, by default ``None``.
    format_explicit : bool, optional
        Whether the format is explicitly specified, by default ``False``.
    pretty : bool, optional
        Whether to pretty-print the output, by default ``True``.
    validate_fn : Any
        The function to call to perform the validation.
    start_command : Any
        The function to call to start the command context for logging and
        telemetry, expected to accept parameters (command, event_format,
        source, target) and return a context object.
    failure_boundary : Any
        The context manager function to use for handling exceptions and logging
        failures, expected to accept parameters (context, source, target) and
        yield a context for the command execution block.
    complete_output : Any
        The function to call to complete the command execution and produce the
        final output, expected to accept parameters (context, data, mode,
        output_path, success_message, result_status, source, status, target,
        valid) and return an integer exit code.
    print_fn : Any, optional
        The function to use for printing messages, by default print.
    stderr : Any, optional
        The stream to use to write error messages, by default
        :obj:`sys.stderr`.

    Returns
    -------
    int
        The CLI exit code.
    """
    source_format_explicit = source_format is not None or format_explicit
    target_label = target or 'stdout'
    context = start_command(
        command='validate',
        event_format=event_format,
        source=source,
        target=target_label,
    )

    with failure_boundary(
        context,
        source=source,
        target=target_label,
    ):
        payload = cast(
            JSONData | str,
            _common_impl.resolve_payload(
                source,
                format_hint=source_format,
                format_explicit=source_format_explicit,
            ),
        )
        rules_payload = _common_impl.resolve_mapping_payload(
            rules,
            format_explicit=source_format_explicit,
            error_message='rules must resolve to a mapping of field rules',
        )
        result = validate_fn(
            payload,
            cast(dict[str, FieldRulesDict], rules_payload),
        )

        if target and target != '-':
            validated_data = result.get('data')
            if validated_data is not None:
                return complete_output(
                    context,
                    validated_data,
                    mode='json_file',
                    output_path=target,
                    success_message='ValidationDict result saved to',
                    result_status='ok',
                    source=source,
                    status='ok',
                    target=target,
                    valid=result.get('valid'),
                )

            print_fn(
                f'ValidationDict failed, no data to save for {target}',
                file=stderr,
            )
            return 0

        return complete_output(
            context,
            result,
            mode='json',
            pretty=pretty,
            result_status='ok',
            source=source,
            status='ok',
            target=target_label,
            valid=result.get('valid'),
        )
