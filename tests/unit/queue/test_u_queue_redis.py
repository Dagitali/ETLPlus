"""
:mod:`tests.unit.queue.test_u_queue_redis` module.

Unit tests for :mod:`etlplus.queue._redis`.
"""

from __future__ import annotations

import pytest

from etlplus.queue import QueueConfigProtocol
from etlplus.queue import QueueService
from etlplus.queue import RedisQueue

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestRedisQueue:
    """Unit tests for :class:`etlplus.queue.RedisQueue`."""

    def test_from_obj_accepts_missing_database(self) -> None:
        """Test Redis database metadata is optional."""
        queue = RedisQueue.from_obj({'name': 'orders'})

        assert isinstance(queue, QueueConfigProtocol)
        assert queue.database is None

    def test_from_obj_accepts_queue_name_and_db_aliases(self) -> None:
        """Test Redis metadata accepts ``queue_name`` and ``db`` aliases."""
        queue = RedisQueue.from_obj({'name': 'orders', 'queue_name': 'events', 'db': 2})

        assert queue.key == 'events'
        assert queue.database == 2

    @pytest.mark.parametrize(
        'database',
        [
            pytest.param('not-an-int', id='string'),
            pytest.param(True, id='bool'),
        ],
    )
    def test_from_obj_rejects_invalid_database(self, database: object) -> None:
        """Test Redis database metadata rejects non-integer values."""
        with pytest.raises(TypeError, match='"database" must be an integer'):
            RedisQueue.from_obj({'name': 'orders', 'database': database})

    def test_from_obj_rejects_negative_database(self) -> None:
        """Test Redis database metadata rejects negative values."""
        with pytest.raises(ValueError, match='greater than or equal to 0'):
            RedisQueue.from_obj({'name': 'orders', 'database': -1})

    def test_from_obj_returns_connector_options(self) -> None:
        """Test Redis queue metadata parsing and option serialization."""
        queue = RedisQueue.from_obj(
            {
                'name': 'orders',
                'url': 'redis://localhost:6379/0',
                'key': 'orders',
                'database': '1',
                'options': {'consumer_group': 'etlplus'},
            },
        )

        assert isinstance(queue, QueueConfigProtocol)
        assert queue.service is QueueService.REDIS
        assert queue.to_connector_options() == {
            'consumer_group': 'etlplus',
            'service': 'redis',
            'url': 'redis://localhost:6379/0',
            'key': 'orders',
            'database': 1,
        }

    def test_modeled_fields_override_options(self) -> None:
        """Test top-level Redis fields take precedence over duplicate options."""
        queue = RedisQueue.from_obj(
            {
                'name': 'orders',
                'key': 'events',
                'database': 1,
                'options': {
                    'service': 'wrong',
                    'key': 'stale',
                    'database': 0,
                },
            },
        )

        assert queue.to_connector_options() == {
            'service': 'redis',
            'key': 'events',
            'database': 1,
        }
