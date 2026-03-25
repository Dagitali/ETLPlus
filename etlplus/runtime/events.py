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

    # -- Static Methods -- #

    @staticmethod
    def build(
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
        return {
            'command': command,
            'event': f'{command}.{lifecycle}',
            'lifecycle': lifecycle,
            'run_id': run_id,
            'schema': EVENT_SCHEMA,
            'schema_version': EVENT_SCHEMA_VERSION,
            'timestamp': timestamp or RuntimeEvents.utc_now_iso(),
        } | fields

    @staticmethod
    def create_run_id() -> str:
        """Return a new stable invocation identifier."""
        return str(uuid4())

    @staticmethod
    def emit(
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

    @staticmethod
    def utc_now_iso() -> str:
        """Return the current UTC timestamp as ISO-8601 text."""
        return datetime.now(UTC).isoformat()
