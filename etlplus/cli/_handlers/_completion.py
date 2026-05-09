"""
:mod:`etlplus.cli._handlers._completion` module.

Shared command-completion helpers for CLI handler implementations.
"""

from __future__ import annotations

from typing import Any
from typing import Literal

from ...utils import PathParser
from . import _lifecycle
from . import _output

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'complete_output',
    # Type Aliases
    'CompletionMode',
]


# SECTION: TYPE ALIASES ===================================================== #


type CompletionMode = Literal['json', 'or_write', 'file', 'json_file']


# SECTION: FUNCTIONS ======================================================== #


def complete_output(
    context: _lifecycle.CommandContext,
    payload: Any,
    *,
    mode: CompletionMode,
    pretty: bool = True,
    output_path: str | None = None,
    format_hint: str | None = None,
    success_message: str | None = None,
    **fields: Any,
) -> int:
    """
    Emit completion for *context* and route the payload by output mode.

    Parameters
    ----------
    context : _lifecycle.CommandContext
        The command context for the completed command.
    payload : Any
        The JSON-serializable payload to emit or write.
    mode : CompletionMode
        The output mode, one of "json", "or_write", "file", or "json_file".
    pretty : bool, optional
        Whether to pretty-print the JSON output when *mode* is "json" or
        "json_file". Defaults to True.
    output_path : str | None, optional
        The file path to write the payload when *mode* is "file" or
        "json_file". Defaults to ``None``, which writes to STDOUT for
        "json_file" mode and is ignored for "file" mode.
    format_hint : str | None, optional
        An optional hint for the file format when *mode* is "file". Ignored
        for other modes. Defaults to ``None``.
    success_message : str | None, optional
        An optional message to print upon successful output when *mode* is
        "or_write" or "file". Ignored for other modes. Defaults to ``None``.
    **fields : Any
        Additional fields to include in the emitted lifecycle event payload.

    Returns
    -------
    int
        The exit code to return after emitting the payload, typically 0 for
        success or 1 for failure depending on the context and output mode.

    Raises
    ------
    AssertionError
        If *mode* is not one of the supported output modes.
    ValueError
        If required parameters for the specified *mode* are missing or invalid.
        For example, if *mode* is "file" but *output_path* is not provided or
        is a STDOUT target, or if *mode* is "or_write" but *success_message* is
        not provided.
    """
    _lifecycle.complete_command(context, **fields)
    match mode:
        case 'json':
            return _output.emit_json_payload(payload, pretty=pretty)
        case 'or_write':
            if not success_message:
                raise ValueError(f'{mode!r} completion requires a success message')
            _output.emit_or_write(
                payload,
                output_path,
                pretty=pretty,
                success_message=success_message,
            )
            return 0
        case 'file':
            if not isinstance(output_path, str) or PathParser.is_stdout_target(
                output_path,
            ):
                raise ValueError(f'{mode!r} completion requires an output path')
            if not success_message:
                raise ValueError(f'{mode!r} completion requires a success message')
            target = output_path
            _output.write_file_payload(
                payload,
                target,
                format_hint=format_hint,
            )
            print(f'{success_message} {target}')
            return 0
        case 'json_file':
            if not success_message:
                raise ValueError(f'{mode!r} completion requires a success message')
            if not _output.write_json_output(
                payload,
                output_path,
                success_message=success_message,
            ):
                return _output.emit_json_payload(payload, pretty=pretty)
            return 0
        case _:
            raise AssertionError(f'Unsupported completion mode: {mode!r}')
