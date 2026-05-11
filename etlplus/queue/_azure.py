"""
:mod:`etlplus.queue._azure` module.

Azure Service Bus queue metadata helpers.
"""

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import ClassVar
from typing import Self
from typing import TypedDict

from ..utils import MappingFieldParser
from ..utils import MappingParser
from ..utils import ValueParser
from ..utils._types import StrAnyMap
from ._enums import QueueService
from ._providers import ProviderQueueConfigMixin

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'AzureServiceBusQueue',
    'AzureServiceBusQueueConfigDict',
]


# SECTION: TYPED DICTS ====================================================== #


class AzureServiceBusQueueConfigDict(TypedDict, total=False):
    """Shape accepted by :meth:`AzureServiceBusQueue.from_obj`."""

    name: str
    namespace: str
    queue_name: str
    topic: str
    subscription: str
    options: StrAnyMap


# SECTION: DATA CLASSES ===================================================== #


@dataclass(kw_only=True, slots=True)
class AzureServiceBusQueue(ProviderQueueConfigMixin):
    """Configuration metadata for Azure Service Bus queues/subscriptions."""

    # -- Instance Attributes -- #

    name: str
    service: QueueService = QueueService.AZURE_SERVICE_BUS
    namespace: str | None = None
    queue_name: str | None = None
    topic: str | None = None
    subscription: str | None = None
    options: dict[str, Any] = field(default_factory=dict)

    # -- Class Attributes -- #

    _option_fields: ClassVar[tuple[str, ...]] = (
        'namespace',
        'queue_name',
        'topic',
        'subscription',
    )

    # -- Class Methods -- #

    @classmethod
    def from_obj(cls, obj: StrAnyMap) -> Self:
        """Parse a mapping into an ``AzureServiceBusQueue`` instance."""
        queue = cls(
            name=MappingFieldParser.require_str(
                obj,
                'name',
                label='AzureServiceBusQueue',
            ),
            namespace=ValueParser.optional_str(obj.get('namespace')),
            queue_name=ValueParser.optional_str(
                obj.get('queue_name', obj.get('queue')),
            ),
            topic=ValueParser.optional_str(obj.get('topic')),
            subscription=ValueParser.optional_str(obj.get('subscription')),
            options=MappingParser.to_dict(obj.get('options')),
        )
        queue.validate()
        return queue

    # -- Instance Methods -- #

    def validate(self) -> None:
        """
        Validate Azure Service Bus queue/topic metadata.

        Raises
        ------
        ValueError
            If queue metadata lacks a queue or topic target, or if a subscription
            is provided without a topic.
        """
        if self.subscription is not None and self.topic is None:
            raise ValueError(
                'AzureServiceBusQueue "subscription" requires "topic"',
            )
        if self.queue_name is None and self.topic is None:
            raise ValueError(
                'AzureServiceBusQueue requires "queue_name" or "topic"',
            )
