"""
:mod:`etlplus.runtime._events` module.

Structured runtime event helpers for CLI execution paths.
"""

from __future__ import annotations

import sys
from datetime import UTC
from datetime import datetime
from typing import Any
from typing import TypedDict
from typing import cast
from uuid import uuid4

from ..utils import JsonCodec

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


# SECTION: INTERNAL TYPED DICTS ============================================= #


class _RuntimeEventBaseDict(TypedDict):
    """Stable base envelope for one structured runtime event."""

    command: str
    event: str
    lifecycle: str
    run_id: str
    schema: str
    schema_version: int
    timestamp: str


class _RuntimeEventDict(_RuntimeEventBaseDict, total=False):
    """Private internal event shape for known stable/additive event fields."""

    config_path: str
    continue_on_fail: bool
    destination: str
    duration_ms: int
    error_message: str
    error_type: str
    etlplus_version: str
    job: str | None
    pipeline_name: str | None
    result_status: str
    run_all: bool
    source: str
    source_type: str
    status: str
    target: str
    target_type: str | None
    valid: bool


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
        event: _RuntimeEventBaseDict = {
            'command': command,
            'event': f'{command}.{lifecycle}',
            'lifecycle': lifecycle,
            'run_id': run_id,
            'schema': EVENT_SCHEMA,
            'schema_version': EVENT_SCHEMA_VERSION,
            'timestamp': timestamp or RuntimeEvents.utc_now_iso(),
        }
        return dict(cast(_RuntimeEventDict, dict(event) | fields))

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
        from ._telemetry import RuntimeTelemetry

        RuntimeTelemetry.emit_event(event)
        if event_format != 'jsonl':
            return
        print(JsonCodec.serialize(event), file=sys.stderr)

    @staticmethod
    def utc_now_iso() -> str:
        """Return the current UTC timestamp as ISO-8601 text."""
        return datetime.now(UTC).isoformat()
