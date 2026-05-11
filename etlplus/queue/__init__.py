"""
:mod:`etlplus.queue` package.

Queue type helpers for message-oriented ETL sources and targets.
"""

from __future__ import annotations

from ._amqp import AmqpQueue
from ._amqp import AmqpQueueConfigDict
from ._aws import AwsSqsQueue
from ._aws import AwsSqsQueueConfigDict
from ._azure import AzureServiceBusQueue
from ._azure import AzureServiceBusQueueConfigDict
from ._base import QueueConfigProtocol
from ._enums import QueueService
from ._enums import QueueType
from ._gcp import GcpPubSubQueue
from ._gcp import GcpPubSubQueueConfigDict
from ._location import QueueLocation
from ._redis import RedisQueue
from ._redis import RedisQueueConfigDict
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
