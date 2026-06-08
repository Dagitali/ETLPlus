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

    @pytest.mark.parametrize(
        ('field_name', 'expected'),
        [
            pytest.param('name', 'orders', id='name'),
            pytest.param('queue_name', 'orders-in', id='queue-alias'),
        ],
    )
    def test_from_obj_accepts_queue_alias(
        self,
        field_name: str,
        expected: str,
    ) -> None:
        """Test Azure queue alias parsing."""
        queue = AzureServiceBusQueue.from_obj(
            {'name': '  orders  ', 'queue': '  orders-in  '},
        )

        assert getattr(queue, field_name) == expected

    @pytest.mark.parametrize(
        ('field_name', 'expected'),
        [
            pytest.param('name', 'orders', id='name'),
            pytest.param('namespace', '123', id='namespace'),
            pytest.param('queue_name', 'orders-in', id='queue-name'),
            pytest.param('topic', 'orders-topic', id='topic'),
            pytest.param('subscription', 'False', id='subscription'),
        ],
    )
    def test_from_obj_normalizes_optional_string_fields(
        self,
        field_name: str,
        expected: str,
    ) -> None:
        """Test Azure Service Bus metadata trims optional target fields."""
        queue = AzureServiceBusQueue.from_obj(
            {
                'name': '  orders  ',
                'namespace': 123,
                'queue_name': '  orders-in  ',
                'topic': '  orders-topic  ',
                'subscription': False,
            },
        )

        assert getattr(queue, field_name) == expected

    @pytest.mark.parametrize(
        ('payload', 'match'),
        [
            (
                {'name': 'orders', 'queue_name': '   '},
                'requires "queue_name" or "topic"',
            ),
            ({'name': 'orders', 'topic': '   '}, 'requires "queue_name" or "topic"'),
            ({'name': 'orders'}, 'requires "queue_name" or "topic"'),
            ({'name': 'orders', 'subscription': 'etlplus'}, 'requires "topic"'),
        ],
    )
    def test_from_obj_rejects_invalid_targets(
        self,
        payload: dict[str, object],
        match: str,
    ) -> None:
        """Test Azure Service Bus metadata requires a valid queue or topic."""
        with pytest.raises(ValueError, match=match):
            AzureServiceBusQueue.from_obj(payload)

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
