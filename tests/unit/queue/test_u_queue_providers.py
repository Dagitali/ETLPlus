"""
:mod:`tests.unit.queue.test_u_queue_providers` module.

Unit tests for :mod:`etlplus.queue._providers`.
"""

from __future__ import annotations

import pytest

from etlplus.queue import AmqpQueue
from etlplus.queue import AzureServiceBusQueue
from etlplus.queue import GcpPubSubQueue
from etlplus.queue import QueueService
from etlplus.queue import RedisQueue

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


type ProviderQueueClass = (
    type[AmqpQueue]
    | type[AzureServiceBusQueue]
    | type[GcpPubSubQueue]
    | type[RedisQueue]
)


# SECTION: TESTS ============================================================ #


class TestProviderQueueConfigs:
    """Unit tests for provider-specific queue config dataclasses."""

    @pytest.mark.parametrize(
        ('queue_cls', 'payload', 'match'),
        [
            pytest.param(
                AmqpQueue,
                {'name': 'orders'},
                'requires "url" or "host"',
                id='amqp-target',
            ),
            pytest.param(
                AzureServiceBusQueue,
                {'name': 'orders', 'subscription': 'etlplus'},
                'requires "topic"',
                id='azure-subscription-topic',
            ),
            pytest.param(
                AzureServiceBusQueue,
                {'name': 'orders'},
                'requires "queue_name" or "topic"',
                id='azure-target',
            ),
            pytest.param(
                GcpPubSubQueue,
                {'name': 'orders', 'subscription': 'etlplus'},
                'requires "project"',
                id='gcp-project',
            ),
            pytest.param(
                GcpPubSubQueue,
                {'name': 'orders', 'project': 'example-project'},
                'requires "topic" or "subscription"',
                id='gcp-target',
            ),
            pytest.param(
                RedisQueue,
                {'name': 'orders', 'database': -1},
                'greater than or equal to 0',
                id='redis-database',
            ),
        ],
    )
    def test_from_obj_rejects_invalid_provider_metadata(
        self,
        queue_cls: ProviderQueueClass,
        payload: dict[str, object],
        match: str,
    ) -> None:
        """Test provider queue configs reject invalid metadata combinations."""
        with pytest.raises(ValueError, match=match):
            queue_cls.from_obj(payload)

    @pytest.mark.parametrize(
        ('queue_cls', 'payload', 'expected_service', 'expected_options'),
        [
            pytest.param(
                AmqpQueue,
                {
                    'name': 'orders',
                    'url': 'amqp://guest:guest@localhost:5672/%2f',
                    'exchange': 'etlplus',
                    'routing_key': 'orders.created',
                    'options': {'durable': True},
                },
                QueueService.AMQP,
                {
                    'durable': True,
                    'service': 'amqp',
                    'url': 'amqp://guest:guest@localhost:5672/%2f',
                    'exchange': 'etlplus',
                    'routing_key': 'orders.created',
                },
                id='amqp',
            ),
            pytest.param(
                AzureServiceBusQueue,
                {
                    'name': 'orders',
                    'namespace': 'example-bus',
                    'queue_name': 'orders-in',
                    'topic': 'orders-topic',
                    'subscription': 'etlplus',
                    'options': {'prefetch_count': 20},
                },
                QueueService.AZURE_SERVICE_BUS,
                {
                    'prefetch_count': 20,
                    'service': 'azure-service-bus',
                    'namespace': 'example-bus',
                    'queue_name': 'orders-in',
                    'topic': 'orders-topic',
                    'subscription': 'etlplus',
                },
                id='azure-service-bus',
            ),
            pytest.param(
                GcpPubSubQueue,
                {
                    'name': 'orders',
                    'project': 'example-project',
                    'topic': 'orders-topic',
                    'subscription': 'etlplus',
                    'options': {'ack_deadline_seconds': 30},
                },
                QueueService.GCP_PUBSUB,
                {
                    'ack_deadline_seconds': 30,
                    'service': 'gcp-pubsub',
                    'project': 'example-project',
                    'topic': 'orders-topic',
                    'subscription': 'etlplus',
                },
                id='gcp-pubsub',
            ),
            pytest.param(
                RedisQueue,
                {
                    'name': 'orders',
                    'url': 'redis://localhost:6379/0',
                    'key': 'orders',
                    'database': '1',
                    'options': {'consumer_group': 'etlplus'},
                },
                QueueService.REDIS,
                {
                    'consumer_group': 'etlplus',
                    'service': 'redis',
                    'url': 'redis://localhost:6379/0',
                    'key': 'orders',
                    'database': 1,
                },
                id='redis',
            ),
        ],
    )
    def test_from_obj_returns_connector_options(
        self,
        queue_cls: ProviderQueueClass,
        payload: dict[str, object],
        expected_service: QueueService,
        expected_options: dict[str, object],
    ) -> None:
        """Test provider queue metadata parsing and option serialization."""
        queue = queue_cls.from_obj(payload)

        assert queue.service is expected_service
        assert queue.to_connector_options() == expected_options

    def test_modeled_provider_fields_override_options(self) -> None:
        """Test top-level provider fields take precedence over duplicate options."""
        queue = AzureServiceBusQueue.from_obj(
            {
                'name': 'orders',
                'queue_name': 'orders',
                'options': {
                    'service': 'wrong',
                    'queue_name': 'stale',
                },
            },
        )

        assert queue.to_connector_options() == {
            'service': 'azure-service-bus',
            'queue_name': 'orders',
        }

    def test_redis_queue_accepts_missing_database(self) -> None:
        """Test Redis database metadata is optional."""
        assert RedisQueue.from_obj({'name': 'orders'}).database is None

    @pytest.mark.parametrize('database', ['not-an-int', True])
    def test_redis_queue_rejects_invalid_database(self, database: object) -> None:
        """Test Redis database metadata rejects non-integer values."""
        with pytest.raises(TypeError, match='"database" must be an integer'):
            RedisQueue.from_obj({'name': 'orders', 'database': database})
