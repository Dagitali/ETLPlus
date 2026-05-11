"""
:mod:`tests.unit.queue.test_u_queue_enums` module.

Unit tests for :mod:`etlplus.queue._enums`.
"""

from __future__ import annotations

import pytest

from etlplus.queue import QueueService
from etlplus.queue import QueueType

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestQueueEnums:
    """Unit tests for queue enum coercion helpers."""

    def test_service_aliases_returns_expected_mapping(self) -> None:
        """Test that :meth:`QueueService.aliases` returns expected aliases."""
        assert QueueService.aliases() == {
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

    @pytest.mark.parametrize(
        ('value', 'expected'),
        [
            ('AWS-SQS', QueueService.AWS_SQS),
            ('amazon-sqs', QueueService.AWS_SQS),
            ('sqs', QueueService.AWS_SQS),
            ('azure-servicebus', QueueService.AZURE_SERVICE_BUS),
            ('google-cloud-pubsub', QueueService.GCP_PUBSUB),
            ('rabbitmq', QueueService.AMQP),
            ('redis-streams', QueueService.REDIS),
        ],
    )
    def test_service_coerce_aliases(
        self,
        value: str,
        expected: QueueService,
    ) -> None:
        """Test that service aliases coerce to expected enum members."""
        assert QueueService.coerce(value) is expected

    def test_service_coerce_rejects_unknown_values(self) -> None:
        """Test that unsupported queue services raise a descriptive error."""
        with pytest.raises(ValueError, match='Invalid QueueService value'):
            QueueService.coerce('kafka')

    def test_type_aliases_returns_expected_mapping(self) -> None:
        """Test that :meth:`QueueType.aliases` returns expected aliases."""
        assert QueueType.aliases() == {
            'default': 'standard',
            'regular': 'standard',
            'sqs': 'standard',
            'first-in-first-out': 'fifo',
            'first_in_first_out': 'fifo',
            'sqs-fifo': 'fifo',
        }

    @pytest.mark.parametrize(
        ('value', 'expected'),
        [
            ('regular', QueueType.STANDARD),
            ('first-in-first-out', QueueType.FIFO),
            ('FIFO', QueueType.FIFO),
        ],
    )
    def test_type_coerce_aliases(
        self,
        value: str,
        expected: QueueType,
    ) -> None:
        """Test that queue type aliases coerce to expected enum members."""
        assert QueueType.coerce(value) is expected

    def test_type_coerce_rejects_unknown_values(self) -> None:
        """Test that unsupported queue types raise a descriptive error."""
        with pytest.raises(ValueError, match='Invalid QueueType value'):
            QueueType.coerce('priority')
