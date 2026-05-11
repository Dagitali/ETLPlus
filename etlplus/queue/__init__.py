"""
:mod:`etlplus.queue` package.

Queue type helpers for message-oriented ETL sources and targets.
"""

from __future__ import annotations

from ._base import QueueConfigProtocol
from ._enums import QueueService
from ._enums import QueueType
from ._location import QueueLocation
from ._providers import AmqpQueue
from ._providers import AmqpQueueConfigDict
from ._providers import AzureServiceBusQueue
from ._providers import AzureServiceBusQueueConfigDict
from ._providers import GcpPubSubQueue
from ._providers import GcpPubSubQueueConfigDict
from ._providers import RedisQueue
from ._providers import RedisQueueConfigDict
from ._sqs import SqsQueue
from ._sqs import SqsQueueConfigDict
from ._types import QueueConfig

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'AmqpQueue',
    'AzureServiceBusQueue',
    'GcpPubSubQueue',
    'QueueLocation',
    'RedisQueue',
    'SqsQueue',
    # Enums
    'QueueService',
    'QueueType',
    # Protocols
    'QueueConfigProtocol',
    # Type Aliases
    'QueueConfig',
    # Typed Dicts
    'AmqpQueueConfigDict',
    'AzureServiceBusQueueConfigDict',
    'GcpPubSubQueueConfigDict',
    'RedisQueueConfigDict',
    'SqsQueueConfigDict',
]
