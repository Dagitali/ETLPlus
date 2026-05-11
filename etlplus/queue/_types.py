"""
:mod:`etlplus.queue._types` module.

Queue type aliases.
"""

from __future__ import annotations

from ._providers import AmqpQueue
from ._providers import AzureServiceBusQueue
from ._providers import GcpPubSubQueue
from ._providers import RedisQueue
from ._sqs import SqsQueue

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Type Aliases
    'QueueConfig',
]


# SECTION: TYPE ALIASES ===================================================== #


type QueueConfig = (
    AmqpQueue | AzureServiceBusQueue | GcpPubSubQueue | RedisQueue | SqsQueue
)
