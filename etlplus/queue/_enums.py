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

    AMQP = 'amqp'
    AWS_SQS = 'aws-sqs'
    AZURE_SERVICE_BUS = 'azure-service-bus'
    GCP_PUBSUB = 'gcp-pubsub'
    REDIS = 'redis'

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
            'aio-pika': 'amqp',
            'amazon-sqs': 'aws-sqs',
            'azure-servicebus': 'azure-service-bus',
            'azure_service_bus': 'azure-service-bus',
            'aws_sqs': 'aws-sqs',
            'gcp-pub-sub': 'gcp-pubsub',
            'google-cloud-pubsub': 'gcp-pubsub',
            'google-pubsub': 'gcp-pubsub',
            'pika': 'amqp',
            'pubsub': 'gcp-pubsub',
            'rabbitmq': 'amqp',
            'redis-streams': 'redis',
            'sqs': 'aws-sqs',
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
