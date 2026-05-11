"""
:mod:`etlplus.queue._enums` module.

Queue service and type enums.
"""

from __future__ import annotations

from ..utils._enums import CoercibleStrEnum
from ..utils._types import StrStrMap

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Enums
    'QueueService',
    'QueueType',
]


# SECTION: ENUMS ============================================================ #


class QueueService(CoercibleStrEnum):
    """Supported queue services."""

    # -- Constants -- #

    SQS = 'sqs'

    # -- Class Methods -- #

    @classmethod
    def aliases(cls) -> StrStrMap:
        """
        Return a mapping of common aliases for each enum member.

        Returns
        -------
        StrStrMap
            A mapping of alias names to their corresponding enum member names.
        """
        return {
            'amazon-sqs': 'sqs',
            'aws-sqs': 'sqs',
            'aws_sqs': 'sqs',
        }


class QueueType(CoercibleStrEnum):
    """Supported queue delivery/ordering types."""

    # -- Constants -- #

    STANDARD = 'standard'
    FIFO = 'fifo'

    # -- Class Methods -- #

    @classmethod
    def aliases(cls) -> StrStrMap:
        """
        Return a mapping of common aliases for each enum member.

        Returns
        -------
        StrStrMap
            A mapping of alias names to their corresponding enum member names.
        """
        return {
            'default': 'standard',
            'regular': 'standard',
            'sqs': 'standard',
            'first-in-first-out': 'fifo',
            'first_in_first_out': 'fifo',
            'sqs-fifo': 'fifo',
        }
