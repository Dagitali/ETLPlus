"""
:mod:`etlplus.queue._base` module.

Shared queue configuration protocols.
"""

from __future__ import annotations

from typing import Any
from typing import ClassVar
from typing import Protocol
from typing import runtime_checkable

from ._enums import QueueService

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Mixins
    'ProviderQueueConfigMixin',
    # Protocols
    'QueueConfigProtocol',
]


# SECTION: MIXINS =========================================================== #


class ProviderQueueConfigMixin:
    """Shared behavior for provider-specific queue config objects."""

    # -- Dunder Instance Attributes -- #

    __slots__ = ()

    # -- Instance Attributes -- #

    service: QueueService
    options: dict[str, Any]

    # -- Internal Instance Attributes -- #

    _option_fields: ClassVar[tuple[str, ...]] = ()

    # -- Instance Methods -- #

    def to_connector_options(self) -> dict[str, Any]:
        """
        Return a connector-friendly options mapping.

        Returns
        -------
        dict[str, Any]
            Queue metadata represented as a plain dictionary.
        """
        data = {**self.options, 'service': self.service.value}
        for field_name in self._option_fields:
            value = getattr(self, field_name)
            if value is not None:
                data[field_name] = value
        return data


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
