"""
:mod:`etlplus.runtime.events` module.

Structured runtime event helpers for CLI execution paths.
"""

from __future__ import annotations

import json
import sys
from datetime import UTC
from datetime import datetime
from typing import Any
from uuid import uuid4

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # COnstants
    'EVENT_SCHEMA',
    'EVENT_SCHEMA_VERSION',
    'build_structured_event',
    # Functions
    'create_run_id',
    'emit_structured_event',
    'utc_now_iso',
]


# SECTION: CONSTANTS ======================================================== #


EVENT_SCHEMA = 'etlplus.event.v1'
EVENT_SCHEMA_VERSION = 1


# SECTION: FUNCTIONS ======================================================== #


def build_structured_event(
    *,
    command: str,
    lifecycle: str,
    run_id: str,
    timestamp: str | None = None,
    **fields: Any,
) -> dict[str, Any]:
    """
    Build a stable structured runtime event envelope.

    Parameters
    ----------
    command : str
        CLI command that emitted the event.
    lifecycle : str
        Lifecycle stage such as ``started``, ``completed``, or ``failed``.
    run_id : str
        Stable invocation identifier for the command run.
    timestamp : str | None, optional
        Explicit timestamp override. Defaults to the current UTC time.
    **fields : Any
        Additional command-specific event fields.

    Returns
    -------
    dict[str, Any]
        Structured event payload.
    """
    event = {
        'command': command,
        'event': f'{command}.{lifecycle}',
        'lifecycle': lifecycle,
        'run_id': run_id,
        'schema': EVENT_SCHEMA,
        'schema_version': EVENT_SCHEMA_VERSION,
        'timestamp': timestamp or utc_now_iso(),
    }
    event.update(fields)
    return event


def create_run_id() -> str:
    """Return a new stable invocation identifier."""
    return str(uuid4())


def emit_structured_event(
    event: dict[str, Any],
    *,
    event_format: str | None,
) -> None:
    """
    Emit one structured runtime event to STDERR.

    Parameters
    ----------
    event : dict[str, Any]
        Event payload to serialize.
    event_format : str | None
        Structured event format selector. Only ``jsonl`` is currently
        supported; falsy values disable emission.
    """
    if event_format != 'jsonl':
        return
    print(
        json.dumps(event, ensure_ascii=False, separators=(',', ':')),
        file=sys.stderr,
    )


def utc_now_iso() -> str:
    """Return the current UTC timestamp as ISO-8601 text."""
    return datetime.now(UTC).isoformat()
