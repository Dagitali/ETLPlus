"""
:mod:`etlplus.queue` package.

Queue type helpers for message-oriented ETL sources and targets.
"""

from __future__ import annotations

from ._aws import AwsSqsQueue
from ._aws import AwsSqsQueueConfigDict
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
from ._types import QueueConfig

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'AmqpQueue',
    'AwsSqsQueue',
    'AzureServiceBusQueue',
    'GcpPubSubQueue',
    'QueueLocation',
    'RedisQueue',
    # Enums
    'QueueService',
    'QueueType',
    # Protocols
    'QueueConfigProtocol',
    # Type Aliases
    'QueueConfig',
    # Typed Dicts
    'AmqpQueueConfigDict',
    'AwsSqsQueueConfigDict',
    'AzureServiceBusQueueConfigDict',
    'GcpPubSubQueueConfigDict',
    'RedisQueueConfigDict',
]
