"""
:mod:`etlplus.cli._handlers.dataops` module.

Data-operation handler implementations for the CLI facade.
"""

from __future__ import annotations

import sys
from collections.abc import Iterator
from collections.abc import Mapping
from contextlib import contextmanager
from typing import Any
from typing import TypeGuard
from typing import cast

from ...ops import extract
from ...ops import load
from ...ops import transform
from ...ops import validate
from ...ops._types import PipelineConfig
from ...ops.validate import FieldRulesDict
from ...ops.validate import validate_schema
from ...utils._types import JSONData
from . import _completion
from . import _input
from . import _lifecycle
from . import _output
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


# SECTION: CLASSES ========================================================== #


class DataCommandPolicy:
    """Own shared command lifecycle, payload resolution, and completion helpers."""

    # -- Static Methods -- #

    @staticmethod
    @contextmanager
    def command_scope(
        *,
        command: str,
        event_format: str | None,
        fields: dict[str, Any],
    ) -> Iterator[_lifecycle.CommandContext]:
        """
        Start a command context and wrap it in the shared failure boundary.

        Parameters
        ----------
        command : str
            Command name for lifecycle events.
        event_format : str | None
            Structured event output format, or ``None`` to disable structured
            events.
        fields : dict[str, Any]
            Additional fields to include in lifecycle events.

        Yields
        ------
        _lifecycle.CommandContext
            Command context for the active command scope.

        Raises
        ------
        Exception
            Any exception raised within the command scope will be caught,
            logged as a command failure event, and re-raised.
        """
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

    @staticmethod
    def complete_success(
        context: _lifecycle.CommandContext,
        payload: Any,
        *,
        mode: str,
        pretty: bool = True,
        result_status: str = 'ok',
        **fields: Any,
    ) -> int:
        """
        Complete a command using the shared successful-status fields.

        Parameters
        ----------
        context : _lifecycle.CommandContext
            Command context for the active command scope.
        payload : Any
            Payload to include in the command output.
        mode : str
            Output mode for the command.
        pretty : bool, optional
            Whether to pretty-print the output.
        result_status : str, optional
            Result status for the command.
        **fields : Any
            Additional fields to include in the command output.

        Returns
        -------
        int
            Exit code for the command.
        """
        return _completion.complete_output(
            context,
            payload,
            mode=mode,
            pretty=pretty,
            result_status=result_status,
            status='ok',
            **fields,
        )

    @classmethod
    def complete_file_success(
        cls,
        context: _lifecycle.CommandContext,
        payload: JSONData,
        *,
        output_path: str,
        format_hint: str | None = None,
        success_message: str,
        **fields: Any,
    ) -> int:
        """
        Complete one command by writing a payload to a concrete target.

        Parameters
        ----------
        context : _lifecycle.CommandContext
            Command context for the active command scope.
        payload : JSONData
            Payload to include in the command output.
        output_path : str
            Path to the output file.
        format_hint : str | None, optional
            Hint for the output format.
        success_message : str
            Message to display on successful completion.
        **fields : Any
            Additional fields to include in the command output.

        Returns
        -------
        int
            Exit code for the command.
        """
        return cls.complete_success(
            context,
            payload,
            mode='file',
            output_path=output_path,
            format_hint=format_hint,
            success_message=success_message,
            **fields,
        )

    @classmethod
    def complete_json_success(
        cls,
        context: _lifecycle.CommandContext,
        payload: Any,
        *,
        pretty: bool = True,
        **fields: Any,
    ) -> int:
        """Complete one command by emitting its payload as JSON.

        Parameters
        ----------
        context : _lifecycle.CommandContext
            Command context for the active command scope.
        payload : Any
            Payload to include in the command output.
        pretty : bool, optional
            Whether to pretty-print the output.
        **fields : Any
            Additional fields to include in the command output.

        Returns
        -------
        int
            Exit code for the command.
        """
        return cls.complete_success(
            context,
            payload,
            mode='json',
            pretty=pretty,
            **fields,
        )

    @staticmethod
    def display_target(
        target: str | None,
    ) -> str:
        """
        Return a human-readable target label for lifecycle events.

        Parameters
        ----------
        target : str | None
            Target identifier, which can be a path, URI, or special value like
            '-' for STDOUT.

        Returns
        -------
        str
            Human-readable label for the target.
        """
        if _output.is_stdout_target(target):
            return 'stdout'
        assert target is not None
        return target

    @staticmethod
    def has_named_target(
        target: str | None,
    ) -> TypeGuard[str]:
        """
        Return whether *target* names a concrete non-STDOUT destination.

        Parameters
        ----------
        target : str | None
            Target identifier, which can be a path, URI, or special value like
            '-' for STDOUT.

        Returns
        -------
        TypeGuard[str]
            ``True`` if *target* names a concrete non-STDOUT destination,
            narrowing *target* to ``str`` in the guarded branch.
        """
        return not _output.is_stdout_target(target)

    @staticmethod
    def is_explicit_format(
        *,
        format_hint: str | None,
        explicit: bool,
    ) -> bool:
        """
        Return True when a format hint should be treated as explicit.

        Parameters
        ----------
        format_hint : str | None
            Format hint for the payload.
        explicit : bool
            Whether the format was explicitly provided.

        Returns
        -------
        bool
            True if the format hint should be treated as explicit, False otherwise.
        """
        return format_hint is not None or explicit

    @classmethod
    def resolve_source_mapping_inputs(
        cls,
        *,
        source: str,
        mapping_payload: JSONData | str,
        source_format: str | None,
        format_explicit: bool,
        error_message: str,
    ) -> tuple[JSONData | str, dict[str, Any]]:
        """
        Resolve a source payload plus a required mapping-style side payload.

        Parameters
        ----------
        source : str
            Source identifier, which can be a path, URI, or special value like
            '-' for STDIN.
        mapping_payload : JSONData | str
            Mapping-style side payload.
        source_format : str | None
            Format hint for the source payload.
        format_explicit : bool
            Whether the format was explicitly provided.
        error_message : str
            Error message to use if resolution fails.

        Returns
        -------
        tuple[JSONData | str, dict[str, Any]]
            Resolved source payload and mapping.
        """
        source_format_explicit = cls.is_explicit_format(
            format_hint=source_format,
            explicit=format_explicit,
        )
        payload = cls.resolve_source_payload(
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

    @staticmethod
    def resolve_source_payload(
        source: str,
        *,
        source_format: str | None,
        format_explicit: bool,
        hydrate_files: bool = True,
    ) -> _ResolvedSourcePayload:
        """
        Resolve one CLI source argument into a loadable payload.

        Parameters
        ----------
        source : str
            Source identifier, which can be a path, URI, or special value like
            '-' for STDIN.
        source_format : str | None
            Format hint for the source payload.
        format_explicit : bool
            Whether the format was explicitly provided.
        hydrate_files : bool, optional
            Whether to hydrate file references. Default is ``True``.

        Returns
        -------
        _ResolvedSourcePayload
            Resolved source payload.
        """
        return cast(
            _ResolvedSourcePayload,
            _payload.resolve_payload(
                source,
                format_hint=source_format,
                format_explicit=format_explicit,
                hydrate_files=hydrate_files,
            ),
        )

    @staticmethod
    def result_status(
        result: object,
        *,
        default: str = 'ok',
    ) -> str:
        """
        Extract a string status field from one result payload.

        Parameters
        ----------
        result : object
            Result payload, typically a dictionary.
        default : str, optional
            Default status to return if extraction fails. Default is ``'ok'``.

        Returns
        -------
        str
            Extracted status or the default value.
        """
        if not isinstance(result, dict):
            return default
        status = result.get('status')
        return cast(str, status) if isinstance(status, str) else default


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _complete_validation_output(
    context: _lifecycle.CommandContext,
    result: Mapping[str, Any],
    *,
    target: str | None,
    file_payload: Any,
    pretty: bool,
    json_fields: dict[str, Any],
    file_fields: dict[str, Any],
) -> int:
    """Complete validation output to a file target or JSON stdout."""
    if DataCommandPolicy.has_named_target(target):
        if file_payload is not None:
            return DataCommandPolicy.complete_success(
                context,
                file_payload,
                mode='json_file',
                output_path=target,
                success_message='ValidationDict result saved to',
                **file_fields,
            )

        print(
            f'ValidationDict failed, no data to save for {target}',
            file=sys.stderr,
        )
        return 0

    return DataCommandPolicy.complete_json_success(
        context,
        result,
        pretty=pretty,
        **json_fields,
    )


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

    with DataCommandPolicy.command_scope(
        command='extract',
        event_format=event_format,
        fields=command_fields,
    ) as context:
        if source == '-':
            payload = _input.parse_text_payload(
                _input.read_stdin_text(),
                source_format,
            )
            return DataCommandPolicy.complete_json_success(
                context,
                payload,
                pretty=pretty,
                **command_fields,
            )

        output_path = target or output
        return DataCommandPolicy.complete_success(
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
    source_format_explicit = DataCommandPolicy.is_explicit_format(
        format_hint=source_format,
        explicit=False,
    )
    target_format_explicit = DataCommandPolicy.is_explicit_format(
        format_hint=target_format,
        explicit=format_explicit,
    )
    command_fields: dict[str, Any] = {
        'source': source,
        'target': DataCommandPolicy.display_target(target),
        'target_type': target_type,
    }

    with DataCommandPolicy.command_scope(
        command='load',
        event_format=event_format,
        fields=command_fields,
    ) as context:
        source_value = DataCommandPolicy.resolve_source_payload(
            source,
            source_format=source_format,
            format_explicit=source_format_explicit,
            hydrate_files=False,
        )

        if target_type == 'file' and target == '-':
            return DataCommandPolicy.complete_json_success(
                context,
                _input.materialize_file_payload(
                    source_value,
                    format_hint=source_format,
                    format_explicit=source_format_explicit,
                ),
                pretty=pretty,
                **command_fields,
            )

        result = load(
            source_value,
            target_type,
            target,
            file_format=target_format if target_format_explicit else None,
        )

        return DataCommandPolicy.complete_success(
            context,
            result,
            mode='or_write',
            output_path=output,
            pretty=pretty,
            success_message='Load result saved to',
            destination=output or 'stdout',
            result_status=DataCommandPolicy.result_status(result),
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
        'target': DataCommandPolicy.display_target(target),
        'target_type': target_type,
    }

    with DataCommandPolicy.command_scope(
        command='transform',
        event_format=event_format,
        fields=command_fields,
    ) as context:
        payload, operations_payload = DataCommandPolicy.resolve_source_mapping_inputs(
            source=source,
            mapping_payload=operations,
            source_format=source_format,
            format_explicit=format_explicit,
            error_message='operations must resolve to a mapping of transforms',
        )
        data = transform(payload, cast(PipelineConfig, operations_payload))

        if DataCommandPolicy.has_named_target(target):
            if target_type not in (None, 'file'):
                resolved_target_type = cast(str, target_type)
                return DataCommandPolicy.complete_json_success(
                    context,
                    load(
                        data,
                        resolved_target_type,
                        target,
                        file_format=target_format if target_format_explicit else None,
                    ),
                    pretty=pretty,
                    source=source,
                    target=target,
                    target_type=resolved_target_type,
                )

            return DataCommandPolicy.complete_file_success(
                context,
                cast(JSONData, data),
                output_path=target,
                format_hint=target_format,
                success_message='Data transformed and saved to',
                source=source,
                target=target,
                target_type=target_type or 'file',
            )

        return DataCommandPolicy.complete_json_success(
            context,
            data,
            pretty=pretty,
            **command_fields,
        )


def validate_handler(
    *,
    source: str,
    rules: JSONData | str,
    schema: str | None = None,
    schema_format: str | None = None,
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
    schema : str | None, optional
        Schema path used for schema-based validation.
    schema_format : str | None, optional
        Schema format override used for schema-based validation.
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
        'target': DataCommandPolicy.display_target(target),
    }
    if schema is not None:
        command_fields['schema'] = schema
        if schema_format is not None:
            command_fields['schema_format'] = schema_format

    with DataCommandPolicy.command_scope(
        command='validate',
        event_format=event_format,
        fields=command_fields,
    ) as context:
        if schema is not None:
            schema_source = (
                _input.read_stdin_text() if _input.is_stdin_source(source) else source
            )
            result = validate_schema(
                schema_source,
                schema,
                schema_format=schema_format,
                source_format=source_format,
            )

            return _complete_validation_output(
                context,
                result,
                target=target,
                file_payload=result,
                pretty=pretty,
                json_fields=command_fields | {'valid': result.get('valid')},
                file_fields={
                    'source': source,
                    'target': target,
                    'valid': result.get('valid'),
                    'schema': schema,
                    'schema_format': schema_format,
                },
            )

        payload, rules_payload = DataCommandPolicy.resolve_source_mapping_inputs(
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

        return _complete_validation_output(
            context,
            result,
            target=target,
            file_payload=result.get('data'),
            pretty=pretty,
            json_fields=command_fields | {'valid': result.get('valid')},
            file_fields={
                'source': source,
                'target': target,
                'valid': result.get('valid'),
            },
        )
