"""
:mod:`etlplus.cli._handlers.extract` module.

Extract-command handler.
"""

from __future__ import annotations

from ...ops import extract
from .. import _io
from .common import _complete_and_emit_json
from .common import _complete_and_emit_or_write
from .common import _fail_command
from .common import _start_command

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'extract_handler',
]


# SECTION:FUNCTIONS ========================================================= #


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
        Type of the source.
    source : str
        Source to extract data from.
    event_format : str | None, optional
        Format of structured events.
    format_hint : str | None, optional
        Hint for inferring the format of the source data.
    format_explicit : bool, optional
        Whether the format was explicitly specified by the user.
    target : str | None, optional
        Target to save the extracted data to.
    output : str | None, optional
        Output path to save the extracted data to (if not using --target).
    pretty : bool, optional
        Whether to pretty-print JSON output.

    Returns
    -------
    int
        Exit code (0 if extraction succeeded, non-zero if any errors occurred).

    Raises
    ------
    Exception
        If any error occurs during extraction.
    """
    explicit_format = format_hint if format_explicit else None
    context = _start_command(
        command='extract',
        event_format=event_format,
        source=source,
        source_type=source_type,
    )

    try:
        if source == '-':
            text = _io.read_stdin_text()
            payload = _io.parse_text_payload(text, format_hint)
            return _complete_and_emit_json(
                context,
                payload,
                pretty=pretty,
                result_status='ok',
                status='ok',
                source=source,
                source_type=source_type,
            )

        result = extract(
            source_type,
            source,
            file_format=explicit_format,
        )
        output_path = target or output

        return _complete_and_emit_or_write(
            context,
            result,
            output_path,
            pretty=pretty,
            success_message='Data extracted and saved to',
            destination=output_path or 'stdout',
            result_status='ok',
            source=source,
            source_type=source_type,
            status='ok',
        )
    except Exception as exc:
        _fail_command(
            context,
            exc,
            source=source,
            source_type=source_type,
        )
        raise
