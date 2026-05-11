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

    def test_from_obj_rejects_missing_connection_target(self) -> None:
        """Test AMQP queue metadata requires a URL or host."""
        with pytest.raises(ValueError, match='requires "url" or "host"'):
            AmqpQueue.from_obj({'name': 'orders'})

    def test_from_obj_returns_connector_options(self) -> None:
        """Test AMQP queue metadata parsing and option serialization."""
        queue = AmqpQueue.from_obj(
            {
                'name': 'orders',
                'url': 'amqp://guest:guest@localhost:5672/%2f',
                'exchange': 'etlplus',
                'routing_key': 'orders.created',
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
