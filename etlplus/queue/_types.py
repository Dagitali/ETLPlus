"""
:mod:`etlplus.queue._types` module.

Queue type aliases.
"""

from __future__ import annotations

from ._amqp import AmqpQueue
from ._aws import AwsSqsQueue
from ._azure import AzureServiceBusQueue
from ._gcp import GcpPubSubQueue
from ._redis import RedisQueue

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Type Aliases
    'QueueConfig',
]


# SECTION: TYPE ALIASES ===================================================== #


type QueueConfig = (
    AmqpQueue | AzureServiceBusQueue | GcpPubSubQueue | RedisQueue | AwsSqsQueue
)
