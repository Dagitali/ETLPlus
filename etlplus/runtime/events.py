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

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'emit_structured_event',
    'utc_now_iso',
]


# SECTION: FUNCTIONS ======================================================== #


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
