"""
:mod:`tests.unit.connector.test_u_connector_queue` module.

Unit tests for :mod:`etlplus.connector._queue`.
"""

from __future__ import annotations

from typing import cast

import pytest

from etlplus.connector._enums import DataConnectorType
from etlplus.connector._queue import ConnectorQueue
from etlplus.queue import AmqpQueue
from etlplus.queue import AwsSqsQueue
from etlplus.queue import AzureServiceBusQueue
from etlplus.queue import GcpPubSubQueue
from etlplus.queue import QueueService
from etlplus.queue import QueueType
from etlplus.queue import RedisQueue

from .pytest_connector_support import assert_connector_fields

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestConnectorQueue:
    """Unit tests for :class:`ConnectorQueue`."""

    def test_fifo_queue_name_must_end_with_fifo_suffix(self) -> None:
        """Test that explicit FIFO queue names require the SQS suffix."""
        with pytest.raises(ValueError, match='must end with ".fifo"'):
            ConnectorQueue.from_obj(
                {
                    'name': 'fifo_events',
                    'type': 'queue',
                    'queue_name': 'events',
                    'queue_type': 'fifo',
                },
            )

    @pytest.mark.parametrize(
        ('payload', 'expected'),
        [
            pytest.param(
                {
                    'name': 'events_queue',
                    'type': 'queue',
                    'service': 'aws-sqs',
                    'queue_name': 'events',
                    'url': 'https://sqs.us-east-1.amazonaws.com/123/events',
                    'region': 'us-east-1',
                    'options': {'visibility_timeout': 30},
                },
                {
                    'type': DataConnectorType.QUEUE,
                    'name': 'events_queue',
                    'service': QueueService.AWS_SQS,
                    'queue_type': QueueType.STANDARD,
                    'queue_name': 'events',
                    'url': 'https://sqs.us-east-1.amazonaws.com/123/events',
                    'region': 'us-east-1',
                    'options': {'visibility_timeout': 30},
                },
                id='standard-sqs',
            ),
            pytest.param(
                {
                    'name': 'fifo_events',
                    'type': 'queue',
                    'queue': 'events.fifo',
                    'region': 123,
                    'options': [('visibility_timeout', 30)],
                },
                {
                    'type': DataConnectorType.QUEUE,
                    'name': 'fifo_events',
                    'service': QueueService.AWS_SQS,
                    'queue_type': QueueType.FIFO,
                    'queue_name': 'events.fifo',
                    'url': None,
                    'region': '123',
                    'options': {},
                },
                id='fifo-sqs',
            ),
        ],
    )
    def test_from_obj_normalizes_queue_fields(
        self,
        payload: dict[str, object],
        expected: dict[str, object],
    ) -> None:
        """Test that :meth:`from_obj` preserves standard queue fields."""
        connector = ConnectorQueue.from_obj(payload)

        assert_connector_fields(connector, expected)

    @pytest.mark.parametrize(
        'payload',
        [
            pytest.param({'type': 'queue'}, id='missing-name'),
            pytest.param({'name': None, 'type': 'queue'}, id='non-string-name'),
        ],
    )
    def test_from_obj_requires_name(
        self,
        payload: dict[str, object],
    ) -> None:
        """Test that :meth:`from_obj` rejects missing or invalid names."""
        with pytest.raises(TypeError, match='ConnectorQueue requires a "name"'):
            ConnectorQueue.from_obj(payload)

    @pytest.mark.parametrize(
        ('payload', 'expected_options'),
        [
            pytest.param(
                {
                    'name': 'events',
                    'type': 'queue',
                    'service': 'aws-sqs',
                    'queue_name': 'events',
                    'url': 'https://sqs.us-east-1.amazonaws.com/123/events',
                    'region': 'us-east-1',
                    'options': {
                        'queue_name': 'stale',
                        'url': 'stale',
                        'region': 'stale',
                    },
                },
                {
                    'service': 'aws-sqs',
                    'queue_type': 'standard',
                    'queue_name': 'events',
                    'url': 'https://sqs.us-east-1.amazonaws.com/123/events',
                    'region': 'us-east-1',
                },
                id='top-level-overrides-options',
            ),
            pytest.param(
                {
                    'name': 'rabbit',
                    'type': 'queue',
                    'service': 'amqp',
                    'options': {
                        'url': 'amqp://guest:guest@localhost:5672/%2f',
                    },
                },
                {
                    'service': 'amqp',
                    'url': 'amqp://guest:guest@localhost:5672/%2f',
                },
                id='missing-top-level-preserves-options',
            ),
        ],
    )
    def test_to_queue_config_field_precedence(
        self,
        payload: dict[str, object],
        expected_options: dict[str, object],
    ) -> None:
        """Test queue config conversion field precedence rules."""
        assert (
            ConnectorQueue.from_obj(payload).to_queue_config().to_connector_options()
            == expected_options
        )

    def test_to_queue_config_rejects_unsupported_service(self) -> None:
        """Test queue config conversion rejects impossible service values."""
        connector = ConnectorQueue(
            name='events',
            service=cast(QueueService, 'unsupported'),
        )

        with pytest.raises(ValueError, match='Unsupported queue service'):
            connector.to_queue_config()

    @pytest.mark.parametrize(
        ('payload', 'expected_cls', 'expected_options'),
        [
            pytest.param(
                {
                    'name': 'events',
                    'type': 'queue',
                    'service': 'aws-sqs',
                    'queue_name': 'events.fifo',
                    'region': 'us-east-1',
                    'options': {'visibility_timeout': 30},
                },
                AwsSqsQueue,
                {
                    'service': 'aws-sqs',
                    'queue_type': 'fifo',
                    'queue_name': 'events.fifo',
                    'region': 'us-east-1',
                    'visibility_timeout': 30,
                },
                id='aws-sqs',
            ),
            pytest.param(
                {
                    'name': 'servicebus',
                    'type': 'queue',
                    'service': 'azure-service-bus',
                    'queue_name': 'orders',
                    'options': {'namespace': 'example-bus'},
                },
                AzureServiceBusQueue,
                {
                    'service': 'azure-service-bus',
                    'namespace': 'example-bus',
                    'queue_name': 'orders',
                },
                id='azure-service-bus',
            ),
            pytest.param(
                {
                    'name': 'pubsub',
                    'type': 'queue',
                    'service': 'gcp-pubsub',
                    'options': {
                        'project': 'example-project',
                        'subscription': 'etlplus',
                    },
                },
                GcpPubSubQueue,
                {
                    'service': 'gcp-pubsub',
                    'project': 'example-project',
                    'subscription': 'etlplus',
                },
                id='gcp-pubsub',
            ),
            pytest.param(
                {
                    'name': 'rabbit',
                    'type': 'queue',
                    'service': 'rabbitmq',
                    'options': {
                        'url': 'amqp://guest:guest@localhost:5672/%2f',
                        'routing_key': 'orders.created',
                    },
                },
                AmqpQueue,
                {
                    'service': 'amqp',
                    'url': 'amqp://guest:guest@localhost:5672/%2f',
                    'routing_key': 'orders.created',
                },
                id='rabbitmq-alias',
            ),
            pytest.param(
                {
                    'name': 'redis',
                    'type': 'queue',
                    'service': 'redis-streams',
                    'queue_name': 'orders',
                    'options': {
                        'database': '2',
                    },
                },
                RedisQueue,
                {
                    'service': 'redis',
                    'key': 'orders',
                    'database': 2,
                },
                id='redis-streams-alias',
            ),
        ],
    )
    def test_to_queue_config_returns_provider_specific_config(
        self,
        payload: dict[str, object],
        expected_cls: type[object],
        expected_options: dict[str, object],
    ) -> None:
        """Test conversion into provider-specific queue config objects."""
        queue_config = ConnectorQueue.from_obj(payload).to_queue_config()

        assert isinstance(queue_config, expected_cls)
        assert queue_config.to_connector_options() == expected_options
