"""
:mod:`etlplus.queue` package.

Queue type helpers for message-oriented ETL sources and targets.
"""

from __future__ import annotations

from ._enums import QueueService
from ._enums import QueueType
from ._sqs import SqsQueue
from ._sqs import SqsQueueConfigDict

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'SqsQueue',
    # Enums
    'QueueService',
    'QueueType',
    # Typed Dicts
    'SqsQueueConfigDict',
]
