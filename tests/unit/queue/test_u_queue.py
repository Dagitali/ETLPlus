"""
:mod:`tests.unit.queue.test_u_queue` module.

Unit tests for :mod:`etlplus.queue`.
"""

from __future__ import annotations

import pytest

import etlplus.queue as queue_pkg
from etlplus.queue import QueueService
from etlplus.queue import QueueType
from etlplus.queue import SqsQueue
from etlplus.queue import SqsQueueConfigDict

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


QUEUE_EXPORTS = [
    ('SqsQueue', SqsQueue),
    ('QueueService', QueueService),
    ('QueueType', QueueType),
    ('SqsQueueConfigDict', SqsQueueConfigDict),
]


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


class TestQueuePackageExports:
    """Unit tests for package-level exports."""

    @pytest.mark.parametrize(('name', 'expected'), QUEUE_EXPORTS)
    def test_expected_symbol_bindings(
        self,
        name: str,
        expected: object,
    ) -> None:
        """Test that package exports resolve to their canonical objects."""
        assert getattr(queue_pkg, name) == expected

    def test_expected_symbols(self) -> None:
        """Test that package facade preserves the documented export order."""
        assert queue_pkg.__all__ == [name for name, _value in QUEUE_EXPORTS]


class TestSqsQueue:
    """Unit tests for :class:`etlplus.queue.SqsQueue`."""

    def test_fifo_queue_name_must_end_with_fifo_suffix(self) -> None:
        """Test that explicit FIFO queues require the SQS ``.fifo`` suffix."""
        with pytest.raises(ValueError, match='must end with ".fifo"'):
            SqsQueue.from_obj({'name': 'events', 'queue_type': 'fifo'})

    @pytest.mark.parametrize(
        ('payload', 'expected_type'),
        [
            pytest.param({'name': 'events'}, QueueType.STANDARD, id='standard'),
            pytest.param({'name': 'events.fifo'}, QueueType.FIFO, id='fifo-suffix'),
            pytest.param(
                {'name': 'events.fifo', 'queue_type': 'fifo'},
                QueueType.FIFO,
                id='explicit-fifo',
            ),
        ],
    )
    def test_from_obj_normalizes_queue_fields(
        self,
        payload: dict[str, object],
        expected_type: QueueType,
    ) -> None:
        """Test that :meth:`from_obj` normalizes SQS queue metadata."""
        connector = SqsQueue.from_obj(
            {
                **payload,
                'url': 123,
                'arn': False,
                'region': 'us-east-1',
                'delay_seconds': '5',
                'max_messages': '10',
                'message_retention_period': 345600,
                'visibility_timeout': '30',
                'wait_time_seconds': '20',
                'dead_letter_queue_arn': 'arn:aws:sqs:us-east-1:123:dead',
                'attributes': {'VisibilityTimeout': '30'},
            },
        )

        assert connector.service is QueueService.AWS_SQS
        assert connector.queue_type is expected_type
        assert connector.url == '123'
        assert connector.arn == 'False'
        assert connector.region == 'us-east-1'
        assert connector.delay_seconds == 5
        assert connector.max_messages == 10
        assert connector.message_retention_period == 345600
        assert connector.visibility_timeout == 30
        assert connector.wait_time_seconds == 20
        assert connector.dead_letter_queue_arn == 'arn:aws:sqs:us-east-1:123:dead'
        assert connector.attributes == {'VisibilityTimeout': '30'}

    def test_from_obj_requires_name(self) -> None:
        """Test that :meth:`from_obj` requires a queue name."""
        with pytest.raises(TypeError, match='SqsQueue requires a "name"'):
            SqsQueue.from_obj({'queue_type': 'fifo'})

    def test_from_obj_rejects_invalid_integer_metadata(self) -> None:
        """Test that integer SQS metadata fields reject non-integer values."""
        with pytest.raises(TypeError, match='"visibility_timeout" must be an integer'):
            SqsQueue.from_obj(
                {
                    'name': 'events',
                    'visibility_timeout': 'not-an-int',
                },
            )

    @pytest.mark.parametrize(
        ('field_name', 'value'),
        [
            pytest.param('delay_seconds', 901, id='delay-seconds'),
            pytest.param('max_messages', 11, id='max-messages'),
            pytest.param(
                'message_retention_period',
                1_209_601,
                id='message-retention-period',
            ),
            pytest.param('visibility_timeout', 43_201, id='visibility-timeout'),
            pytest.param('wait_time_seconds', 21, id='wait-time-seconds'),
        ],
    )
    def test_from_obj_rejects_out_of_range_integer_metadata(
        self,
        field_name: str,
        value: int,
    ) -> None:
        """Test that bounded SQS metadata fields enforce AWS limits."""
        with pytest.raises(ValueError, match=f'"{field_name}" must be between'):
            SqsQueue.from_obj(
                {
                    'name': 'events',
                    field_name: value,
                },
            )

    def test_standard_queue_rejects_fifo_only_metadata(self) -> None:
        """Test that standard queues reject FIFO-only message metadata."""
        with pytest.raises(ValueError, match='FIFO fields require'):
            SqsQueue.from_obj(
                {
                    'name': 'events',
                    'message_group_id': 'events',
                },
            )

    def test_to_connector_options_returns_plain_mapping(self) -> None:
        """Test that queue metadata can be exposed as connector options."""
        queue = SqsQueue.from_obj(
            {
                'name': 'events.fifo',
                'url': 'https://sqs.us-east-1.amazonaws.com/123/events.fifo',
                'region': 'us-east-1',
                'visibility_timeout': 30,
                'wait_time_seconds': 20,
                'content_based_deduplication': True,
                'message_group_id': 'events',
                'attributes': {'ContentBasedDeduplication': 'true'},
            },
        )

        assert queue.to_connector_options() == {
            'ContentBasedDeduplication': 'true',
            'service': 'aws-sqs',
            'queue_type': 'fifo',
            'queue_name': 'events.fifo',
            'url': 'https://sqs.us-east-1.amazonaws.com/123/events.fifo',
            'region': 'us-east-1',
            'visibility_timeout': 30,
            'wait_time_seconds': 20,
            'content_based_deduplication': True,
            'message_group_id': 'events',
        }
