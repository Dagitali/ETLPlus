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

    def test_from_obj_normalizes_optional_string_fields(self) -> None:
        """Test Redis metadata trims optional string fields."""
        queue = RedisQueue.from_obj(
            {
                'name': '  orders  ',
                'url': 123,
                'key': '  events  ',
            },
        )

        assert queue.name == 'orders'
        assert queue.url == '123'
        assert queue.key == 'events'

    @pytest.mark.parametrize(
        ('database', 'expected_exc', 'match'),
        [
            ('not-an-int', TypeError, '"database" must be an integer'),
            (True, TypeError, '"database" must be an integer'),
            (1.5, TypeError, '"database" must be an integer'),
            (-1, ValueError, 'greater than or equal to 0'),
        ],
    )
    def test_from_obj_rejects_invalid_database(
        self,
        database: object,
        expected_exc: type[Exception],
        match: str,
    ) -> None:
        """Test Redis database metadata rejects invalid values."""
        with pytest.raises(expected_exc, match=match):
            RedisQueue.from_obj({'name': 'orders', 'database': database})

    @pytest.mark.parametrize(
        'payload',
        [
            {'url': 'redis://localhost:6379/0'},
            {'name': '   ', 'url': 'redis://localhost:6379/0'},
        ],
    )
    def test_from_obj_requires_name(self, payload: dict[str, object]) -> None:
        """Test that :meth:`from_obj` requires a queue metadata name."""
        with pytest.raises(TypeError, match='RedisQueue requires a "name"'):
            RedisQueue.from_obj(payload)

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

    def test_to_connector_options_omits_empty_optional_fields(self) -> None:
        """Test that empty optional Redis metadata does not appear in options."""
        assert RedisQueue.from_obj({'name': 'orders'}).to_connector_options() == {
            'service': 'redis',
        }
