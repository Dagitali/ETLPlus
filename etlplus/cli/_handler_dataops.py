"""
:mod:`etlplus.cli._handler_dataops` module.

Data-operation handler implementations for the CLI facade.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any
from typing import cast

from ..ops.validate import FieldRulesDict
from ..utils._types import JSONData

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
    io_module: Any,
    start_command_fn: Any,
    failure_boundary_fn: Any,
    complete_output_fn: Any,
) -> int:
    """
    Extract data from a source.

    Parameters
    ----------
    source_type : str
        The type of the source (e.g. 'file', 'database', 'api').
    source : str
        The source identifier (e.g. file path, connection string, API endpoint).
    event_format : str | None, optional
        The expected format of the source data for event logging purposes.
    format_hint : str | None, optional
        A hint for the data format to assist with parsing when the format is
        not explicit.
    format_explicit : bool, optional
        Whether the format is explicitly specified (e.g. via a CLI option) and
        should be used as-is without inference.
    target : str | None, optional
        The target identifier for the extracted data, used for logging and
        output handling.
    output : str | None, optional
        The file path to write the extracted data to, or None to write to stdout.
    pretty : bool, optional
        Whether to pretty-print the output when writing JSON to stdout.
    extract_fn : Any
        The function to perform the actual data extraction, which should accept
        parameters (source_type, source, file_format) and return the extracted data.
    io_module : Any
        The module providing I/O utilities for reading from stdin and writing
        output, expected to have methods :func:`read_stdin_text` and
        :func:`parse_text_payload`.
    start_command_fn : Any
        The function to call to start the command context for logging and
        telemetry, expected to accept parameters (command, event_format,
        source, source_type) and return a context object.
    failure_boundary_fn : Any
        The context manager function to use for handling exceptions and logging
        failures, expected to accept parameters (context, source, source_type)
        and yield a context for the command execution block.
    complete_output_fn : Any
        The function to call to complete the command with output, expected to
        accept parameters (context, payload, mode, pretty, result_status,
        source, status, target, target_type) and return an exit code.

    Returns
    -------
    int
        The exit code of the command.
    """
    explicit_format = format_hint if format_explicit else None
    context = start_command_fn(
        command='extract',
        event_format=event_format,
        source=source,
        source_type=source_type,
    )

    with failure_boundary_fn(
        context,
        source=source,
        source_type=source_type,
    ):
        if source == '-':
            text = io_module.read_stdin_text()
            payload = io_module.parse_text_payload(
                text,
                format_hint,
            )
            return complete_output_fn(
                context,
                payload,
                mode='json',
                pretty=pretty,
                result_status='ok',
                status='ok',
                source=source,
                source_type=source_type,
            )

        result = extract_fn(
            source_type,
            source,
            file_format=explicit_format,
        )
        output_path = target or output

        return complete_output_fn(
            context,
            result,
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
    io_module: Any,
    start_command_fn: Any,
    failure_boundary_fn: Any,
    resolve_payload_fn: Any,
    complete_output_fn: Any,
) -> int:
    """
    Load data into a target.

    Parameters
    ----------
    source : str
        The source identifier for the data to load (e.g. file path, JSON string).
    target_type : str
        The type of the target (e.g. 'file', 'database', 'api').
    target : str
        The target identifier (e.g. file path, connection string, API endpoint).
    event_format : str | None, optional
        The expected format of the source data for event logging purposes.
    source_format : str | None, optional
        The format of the source data to assist with parsing when the format is
        not explicit.
    target_format : str | None, optional
        The format to write the data in when loading, if applicable.
    format_explicit : bool, optional
        Whether the source format is explicitly specified (e.g. via a CLI
        option) and should be used as-is without inference.
    output : str | None, optional
        The file path to write the load result to, or None to write to stdout.
    pretty : bool, optional
        Whether to pretty-print the output when writing JSON to stdout.
    load_fn : Any
        The function to perform the actual data loading, which should accept
        parameters (data, target_type, target, file_format) and return a result
        dict or status.
    io_module : Any
        The module providing I/O utilities for parsing payloads, expected to
        have a method :func:`parse_text_payload`.
    start_command_fn : Any
        The function to call to start the command context for logging and
        telemetry, expected to accept parameters (command, event_format,
        source, target, target_type) and return a context object.
    failure_boundary_fn : Any
        The context manager function to use for handling exceptions and logging
        failures, expected to accept parameters (context, source, target,
        target_type) and yield a context for the command execution block.
    resolve_payload_fn : Any
        The function to call to resolve the source data into a payload,
        expected to accept parameters (source, format_hint, format_explicit,
        hydrate_files) and return the resolved data.
    complete_output_fn : Any
        The function to call to complete the command with output, expected to
        accept parameters (context, payload, mode, pretty, result_status,
        source, status, target, target_type) and return an exit code.

    Returns
    -------
    int
        The exit code of the command execution.
    """
    explicit_format = target_format if format_explicit else None
    context = start_command_fn(
        command='load',
        event_format=event_format,
        source=source,
        target=target,
        target_type=target_type,
    )

    with failure_boundary_fn(
        context,
        source=source,
        target=target,
        target_type=target_type,
    ):
        source_value = cast(
            str | Path | os.PathLike[str] | dict[str, Any] | list[dict[str, Any]],
            resolve_payload_fn(
                source,
                format_hint=source_format,
                format_explicit=source_format is not None,
                hydrate_files=False,
            ),
        )

        if target_type == 'file' and target == '-':
            payload = io_module.materialize_file_payload(
                source_value,
                format_hint=source_format,
                format_explicit=source_format is not None,
            )
            return complete_output_fn(
                context,
                payload,
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
            file_format=explicit_format,
        )

        return complete_output_fn(
            context,
            result,
            mode='or_write',
            output_path=output,
            pretty=pretty,
            success_message='Load result saved to',
            destination=output or 'stdout',
            result_status=result.get('status') if isinstance(result, dict) else 'ok',
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
    start_command_fn: Any,
    failure_boundary_fn: Any,
    resolve_payload_fn: Any,
    resolve_mapping_payload_fn: Any,
    complete_output_fn: Any,
) -> int:
    """
    Transform data from a source and optionally write the result.

    Parameters
    ----------
    source : str
        The source identifier for the data to transform (e.g. file path, JSON string).
    operations : JSONData | str
        The transformation operations to apply, either as a JSON-like dict or a
        string that resolves to such a dict.
    target : str | None, optional
        The target identifier for the transformed data, used for logging and
        output handling.
    target_type : str | None, optional
        The type of the target (e.g. 'file', 'database', 'api'), used for
        logging and output handling.
    event_format : str | None, optional
        The expected format of the source data for event logging purposes.
    source_format : str | None, optional
        The format of the source data to assist with parsing when the format is
        not explicit.
    target_format : str | None, optional
        The format to write the data in when loading, if applicable.
    pretty : bool, optional
        Whether to pretty-print the output when writing JSON to stdout.
    format_explicit : bool, optional
        Whether the source format is explicitly specified (e.g. via a CLI
        option) and should be used as-is without inference.
    load_fn : Any
        The function to perform the actual data loading, which should accept
        parameters (data, target_type, target, file_format) and return a result
        dict or status.
    transform_fn : Any
        The function to perform the actual data transformation, which should
        accept parameters (source_data, operations) and return the transformed
        data.
    start_command_fn : Any
        The function to call to start the command context for logging and
        telemetry, expected to accept parameters (command, event_format,
        source, target, target_type) and return a context object.
    failure_boundary_fn : Any
        The context manager function to use for handling exceptions and logging
        failures, expected to accept parameters (context, source, target,
        target_type) and yield a context for the command execution block.
    resolve_payload_fn : Any
        The function to call to resolve the source data into a payload,
        expected to accept parameters (source, format_hint, format_explicit,
        hydrate_files) and return the resolved data.
    resolve_mapping_payload_fn : Any
        The function to call to resolve the operations into a mapping payload,
        expected to accept parameters (operations, format_explicit,
        error_message) and return the resolved mapping.
    complete_output_fn : Any
        The function to call to complete the command with output, expected to
        accept parameters (context, payload, mode, pretty, result_status,
        source, status, target, target_type) and return an exit code.

    Returns
    -------
    int
        The exit code of the command execution.
    """
    source_format_explicit = source_format is not None or format_explicit
    context = start_command_fn(
        command='transform',
        event_format=event_format,
        source=source,
        target=target or 'stdout',
        target_type=target_type,
    )

    with failure_boundary_fn(
        context,
        source=source,
        target=target or 'stdout',
        target_type=target_type,
    ):
        payload = cast(
            JSONData | str,
            resolve_payload_fn(
                source,
                format_hint=source_format,
                format_explicit=source_format_explicit,
            ),
        )

        operations_payload = resolve_mapping_payload_fn(
            operations,
            format_explicit=source_format_explicit,
            error_message='operations must resolve to a mapping of transforms',
        )

        data = transform_fn(payload, operations_payload)

        if target and target != '-':
            if target_type not in (None, 'file'):
                resolved_target_type = cast(str, target_type)
                result = load_fn(
                    data,
                    resolved_target_type,
                    target,
                    file_format=target_format if source_format_explicit else None,
                )
                return complete_output_fn(
                    context,
                    result,
                    mode='json',
                    pretty=pretty,
                    result_status='ok',
                    source=source,
                    status='ok',
                    target=target,
                    target_type=resolved_target_type,
                )

            return complete_output_fn(
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

        return complete_output_fn(
            context,
            data,
            mode='json',
            pretty=pretty,
            result_status='ok',
            source=source,
            status='ok',
            target=target or 'stdout',
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
    start_command_fn: Any,
    failure_boundary_fn: Any,
    resolve_payload_fn: Any,
    resolve_mapping_payload_fn: Any,
    complete_output_fn: Any,
    print_fn: Any = print,
    stderr: Any = sys.stderr,
) -> int:
    """
    Validate data from a source.

    Parameters
    ----------
    source : str
        The source identifier for the data to validate (e.g. file path, JSON
        string).
    rules : JSONData | str
        The validation rules to apply, either as a JSON-like dict or a string
        that resolves to such a dict.
    event_format : str | None, optional
        The expected format of the source data for event logging purposes.
    source_format : str | None, optional
        The format of the source data to assist with parsing when the format is
        not explicit.
    target : str | None, optional
        The target identifier for the validation result, used for logging and
        output handling.
    format_explicit : bool, optional
        Whether the source format is explicitly specified (e.g. via a CLI
        option) and should be used as-is without inference.
    pretty : bool, optional
        Whether to pretty-print the output when writing JSON to stdout.
    validate_fn : Any
        The function to perform the actual data validation, which should accept
        parameters (data, rules) and return a dict with validation results.
    start_command_fn : Any
        The function to call to start the command context for logging and
        telemetry, expected to accept parameters (command, event_format,
        source, target) and return a context object.
    failure_boundary_fn : Any
        The context manager function to use for handling exceptions and logging
        failures, expected to accept parameters (context, source, target) and
        yield a context for the command execution block.
    resolve_payload_fn : Any
        The function to call to resolve the source data into a payload,
        expected to accept parameters (source, format_hint, format_explicit,
        hydrate_files) and return the resolved data.
    resolve_mapping_payload_fn : Any
        The function to call to resolve the rules into a mapping payload,
        expected to accept parameters (rules, format_explicit, error_message)
        and return the resolved mapping.
    complete_output_fn : Any
        The function to call to complete the command with output, expected to
        accept parameters (context, payload, mode, pretty, result_status,
        source, status, target, valid) and return an exit code.
    print_fn : Any, optional
        The function to use for printing messages, defaulting to the built-in
        :func:`print`.
    stderr : Any, optional
        The stream to write error messages to, defaulting to :obj:`sys.stderr`.

    Returns
    -------
    int
        The exit code of the validation command.
    """
    context = start_command_fn(
        command='validate',
        event_format=event_format,
        source=source,
        target=target or 'stdout',
    )

    with failure_boundary_fn(
        context,
        source=source,
        target=target or 'stdout',
    ):
        source_format_explicit = source_format is not None or format_explicit
        payload = cast(
            JSONData | str,
            resolve_payload_fn(
                source,
                format_hint=source_format,
                format_explicit=source_format_explicit,
            ),
        )

        rules_payload = resolve_mapping_payload_fn(
            rules,
            format_explicit=source_format_explicit,
            error_message='rules must resolve to a mapping of field rules',
        )

        field_rules = cast(dict[str, FieldRulesDict], rules_payload)
        result = validate_fn(payload, field_rules)

        if target and target != '-':
            validated_data = result.get('data')
            if validated_data is not None:
                return complete_output_fn(
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

        return complete_output_fn(
            context,
            result,
            mode='json',
            pretty=pretty,
            result_status='ok',
            source=source,
            status='ok',
            target=target or 'stdout',
            valid=result.get('valid'),
        )
