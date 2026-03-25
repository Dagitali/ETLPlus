"""
:mod:`etlplus.runtime.events` module.

Structured runtime event helpers for CLI execution paths.
"""

from __future__ import annotations

import sys
from datetime import UTC
from datetime import datetime
from typing import Any
from uuid import uuid4

from ..utils.data import serialize_json

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'RuntimeEvents',
    # Constants
    'EVENT_SCHEMA',
    'EVENT_SCHEMA_VERSION',
]


# SECTION: CONSTANTS ======================================================== #


EVENT_SCHEMA = 'etlplus.event.v1'
EVENT_SCHEMA_VERSION = 1


# SECTION: FUNCTIONS ======================================================== #


class RuntimeEvents:
    """Shared factory and emitter helpers for structured runtime events."""

    # -- Class Methods -- #

    @classmethod
    def build(
        cls,
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
            'timestamp': timestamp or cls.utc_now_iso(),
        }
        event.update(fields)
        return event

    @classmethod
    def create_run_id(cls) -> str:
        """Return a new stable invocation identifier."""
        return str(uuid4())

    @classmethod
    def emit(
        cls,
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
        print(serialize_json(event), file=sys.stderr)

    @classmethod
    def utc_now_iso(cls) -> str:
        """Return the current UTC timestamp as ISO-8601 text."""
        return datetime.now(UTC).isoformat()
