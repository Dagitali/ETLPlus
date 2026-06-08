"""
:mod:`tests.unit.queue.test_u_queue_amqp` module.

Unit tests for :mod:`etlplus.queue._amqp`.
"""

from __future__ import annotations

import pytest

from etlplus.queue import AmqpQueue
from etlplus.queue import QueueConfigProtocol
from etlplus.queue import QueueService

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestAmqpQueue:
    """Unit tests for :class:`etlplus.queue.AmqpQueue`."""

    @pytest.mark.parametrize(
        ('field_name', 'expected'),
        [
            pytest.param('name', 'orders', id='name'),
            pytest.param('url', '123', id='url'),
            pytest.param('host', 'localhost', id='host'),
            pytest.param('virtual_host', '/', id='virtual-host'),
            pytest.param('exchange', 'etlplus', id='exchange'),
            pytest.param('routing_key', 'False', id='routing-key'),
        ],
    )
    def test_from_obj_normalizes_optional_string_fields(
        self,
        field_name: str,
        expected: str,
    ) -> None:
        """Test AMQP metadata trims optional string fields."""
        queue = AmqpQueue.from_obj(
            {
                'name': '  orders  ',
                'url': 123,
                'host': '  localhost  ',
                'virtual_host': '  /  ',
                'exchange': '  etlplus  ',
                'routing_key': False,
            },
        )

        assert getattr(queue, field_name) == expected

    @pytest.mark.parametrize(
        'payload',
        [
            {'name': 'orders', 'url': '   '},
            {'name': 'orders', 'host': '   '},
            {'name': 'orders'},
        ],
    )
    def test_from_obj_rejects_invalid_connection_targets(
        self,
        payload: dict[str, object],
    ) -> None:
        """Test AMQP queue metadata requires a valid URL or host."""
        with pytest.raises(ValueError, match='requires "url" or "host"'):
            AmqpQueue.from_obj(payload)

    @pytest.mark.parametrize(
        ('field_name', 'expected'),
        [
            pytest.param('service', QueueService.AMQP, id='service'),
            pytest.param(
                'connector_options',
                {
                    'durable': True,
                    'service': 'amqp',
                    'url': 'amqp://guest:guest@localhost:5672/%2f',
                    'exchange': 'etlplus',
                    'routing_key': 'orders.created',
                },
                id='connector-options',
            ),
        ],
    )
    def test_from_obj_returns_connector_options(
        self,
        field_name: str,
        expected: object,
    ) -> None:
        """Test AMQP queue metadata parsing and option serialization."""
        queue = AmqpQueue.from_obj(
            {
                'name': '  orders  ',
                'url': '  amqp://guest:guest@localhost:5672/%2f  ',
                'exchange': '  etlplus  ',
                'routing_key': '  orders.created  ',
                'options': {'durable': True},
            },
        )

        assert isinstance(queue, QueueConfigProtocol)
        actual = (
            queue.to_connector_options()
            if field_name == 'connector_options'
            else getattr(queue, field_name)
        )
        assert actual == expected
