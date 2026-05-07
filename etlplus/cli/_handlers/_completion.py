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


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _require_file_target(
    output_path: str | None,
    *,
    mode: CompletionMode,
) -> str:
    """Return a concrete file target or fail with a clear completion error."""
    if isinstance(output_path, str) and not PathParser.is_stdout_target(output_path):
        return output_path
    raise ValueError(f'{mode!r} completion requires an output path')


def _require_success_message(
    success_message: str | None,
    *,
    mode: CompletionMode,
) -> str:
    """Return a completion success message or fail with a clear error."""
    if success_message:
        return success_message
    raise ValueError(f'{mode!r} completion requires a success message')


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
    """
    _lifecycle.complete_command(context, **fields)
    match mode:
        case 'json':
            return _output.emit_json_payload(payload, pretty=pretty)
        case 'or_write':
            _output.emit_or_write(
                payload,
                output_path,
                pretty=pretty,
                success_message=_require_success_message(
                    success_message,
                    mode=mode,
                ),
            )
            return 0
        case 'file':
            target = _require_file_target(output_path, mode=mode)
            _output.write_file_payload(
                payload,
                target,
                format_hint=format_hint,
            )
            print(f'{_require_success_message(success_message, mode=mode)} {target}')
            return 0
        case 'json_file':
            if not _output.write_json_output(
                payload,
                output_path,
                success_message=_require_success_message(
                    success_message,
                    mode=mode,
                ),
            ):
                return _output.emit_json_payload(payload, pretty=pretty)
            return 0
        case _:
            raise AssertionError(f'Unsupported completion mode: {mode!r}')
