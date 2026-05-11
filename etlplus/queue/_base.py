"""
:mod:`etlplus.queue._base` module.

Shared queue configuration protocols.
"""

from __future__ import annotations

from typing import Any
from typing import Protocol
from typing import runtime_checkable

from ._enums import QueueService

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Protocols
    'QueueConfigProtocol',
]


# SECTION: PROTOCOLS ======================================================== #


@runtime_checkable
class QueueConfigProtocol(Protocol):
    """Structural contract for provider-specific queue configuration objects."""

    # -- Attributes -- #

    name: str
    service: QueueService

    # -- Instance Methods -- #

    def to_connector_options(self) -> dict[str, Any]:
        """
        Return a connector-friendly options mapping.

        Returns
        -------
        dict[str, Any]
            Queue metadata represented as a plain dictionary.
        """
        ...
