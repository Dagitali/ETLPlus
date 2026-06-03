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

    def test_from_obj_normalizes_optional_string_fields(self) -> None:
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

        assert queue.name == 'orders'
        assert queue.url == '123'
        assert queue.host == 'localhost'
        assert queue.virtual_host == '/'
        assert queue.exchange == 'etlplus'
        assert queue.routing_key == 'False'

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
        'payload',
        [
            {'url': 'amqp://guest:guest@localhost:5672/%2f'},
            {'name': '   ', 'url': 'amqp://guest:guest@localhost:5672/%2f'},
        ],
    )
    def test_from_obj_requires_name(self, payload: dict[str, object]) -> None:
        """Test that :meth:`from_obj` requires a queue metadata name."""
        with pytest.raises(TypeError, match='AmqpQueue requires a "name"'):
            AmqpQueue.from_obj(payload)

    def test_from_obj_returns_connector_options(self) -> None:
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
        assert queue.service is QueueService.AMQP
        assert queue.to_connector_options() == {
            'durable': True,
            'service': 'amqp',
            'url': 'amqp://guest:guest@localhost:5672/%2f',
            'exchange': 'etlplus',
            'routing_key': 'orders.created',
        }

    def test_to_connector_options_omits_empty_optional_fields(self) -> None:
        """Test that empty optional AMQP metadata does not appear in options."""
        assert AmqpQueue.from_obj(
            {'name': 'orders', 'host': 'localhost'},
        ).to_connector_options() == {
            'service': 'amqp',
            'host': 'localhost',
        }
