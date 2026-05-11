"""
:mod:`tests.unit.queue.test_u_queue_azure` module.

Unit tests for :mod:`etlplus.queue._azure`.
"""

from __future__ import annotations

import pytest

from etlplus.queue import AzureServiceBusQueue
from etlplus.queue import QueueConfigProtocol
from etlplus.queue import QueueService

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestAzureServiceBusQueue:
    """Unit tests for :class:`etlplus.queue.AzureServiceBusQueue`."""

    def test_from_obj_accepts_queue_alias(self) -> None:
        """Test Azure Service Bus queue metadata accepts the ``queue`` alias."""
        queue = AzureServiceBusQueue.from_obj({'name': 'orders', 'queue': 'orders-in'})

        assert queue.queue_name == 'orders-in'
        assert queue.to_connector_options() == {
            'service': 'azure-service-bus',
            'queue_name': 'orders-in',
        }

    def test_from_obj_rejects_missing_target(self) -> None:
        """Test Azure Service Bus metadata requires a queue or topic."""
        with pytest.raises(ValueError, match='requires "queue_name" or "topic"'):
            AzureServiceBusQueue.from_obj({'name': 'orders'})

    def test_from_obj_rejects_subscription_without_topic(self) -> None:
        """Test Azure Service Bus subscriptions require a topic."""
        with pytest.raises(ValueError, match='requires "topic"'):
            AzureServiceBusQueue.from_obj(
                {'name': 'orders', 'subscription': 'etlplus'},
            )

    def test_from_obj_returns_connector_options(self) -> None:
        """Test Azure Service Bus metadata parsing and option serialization."""
        queue = AzureServiceBusQueue.from_obj(
            {
                'name': 'orders',
                'namespace': 'example-bus',
                'queue_name': 'orders-in',
                'topic': 'orders-topic',
                'subscription': 'etlplus',
                'options': {'prefetch_count': 20},
            },
        )

        assert isinstance(queue, QueueConfigProtocol)
        assert queue.service is QueueService.AZURE_SERVICE_BUS
        assert queue.to_connector_options() == {
            'prefetch_count': 20,
            'service': 'azure-service-bus',
            'namespace': 'example-bus',
            'queue_name': 'orders-in',
            'topic': 'orders-topic',
            'subscription': 'etlplus',
        }

    def test_modeled_fields_override_options(self) -> None:
        """Test top-level Azure fields take precedence over duplicate options."""
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
