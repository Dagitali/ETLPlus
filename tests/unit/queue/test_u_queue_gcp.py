"""
:mod:`tests.unit.queue.test_u_queue_gcp` module.

Unit tests for :mod:`etlplus.queue._gcp`.
"""

from __future__ import annotations

import pytest

from etlplus.queue import GcpPubSubQueue
from etlplus.queue import QueueConfigProtocol
from etlplus.queue import QueueService

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestGcpPubSubQueue:
    """Unit tests for :class:`etlplus.queue.GcpPubSubQueue`."""

    @pytest.mark.parametrize(
        ('field_name', 'expected'),
        [
            pytest.param('name', 'orders', id='name'),
            pytest.param('project', '123', id='project'),
            pytest.param('topic', 'orders-topic', id='topic'),
            pytest.param('subscription', 'False', id='subscription'),
        ],
    )
    def test_from_obj_normalizes_optional_string_fields(
        self,
        field_name: str,
        expected: str,
    ) -> None:
        """Test Google Cloud Pub/Sub metadata trims optional target fields."""
        queue = GcpPubSubQueue.from_obj(
            {
                'name': '  orders  ',
                'project': 123,
                'topic': '  orders-topic  ',
                'subscription': False,
            },
        )

        assert getattr(queue, field_name) == expected

    @pytest.mark.parametrize(
        ('payload', 'match'),
        [
            ({'name': 'orders', 'subscription': 'etlplus'}, 'requires "project"'),
            (
                {'name': 'orders', 'project': '   ', 'subscription': 'etlplus'},
                'requires "project"',
            ),
            (
                {'name': 'orders', 'project': 'example-project'},
                'requires "topic" or "subscription"',
            ),
            (
                {'name': 'orders', 'project': 'example-project', 'topic': '   '},
                'requires "topic" or "subscription"',
            ),
            (
                {
                    'name': 'orders',
                    'project': 'example-project',
                    'subscription': '   ',
                },
                'requires "topic" or "subscription"',
            ),
        ],
    )
    def test_from_obj_rejects_invalid_targets(
        self,
        payload: dict[str, object],
        match: str,
    ) -> None:
        """Test Google Cloud Pub/Sub metadata requires valid target fields."""
        with pytest.raises(ValueError, match=match):
            GcpPubSubQueue.from_obj(payload)

    @pytest.mark.parametrize(
        ('field_name', 'expected'),
        [
            pytest.param('service', QueueService.GCP_PUBSUB, id='service'),
            pytest.param(
                'connector_options',
                {
                    'ack_deadline_seconds': 30,
                    'service': 'gcp-pubsub',
                    'project': 'example-project',
                    'topic': 'orders-topic',
                    'subscription': 'etlplus',
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
        """Test Google Cloud Pub/Sub metadata parsing and option serialization."""
        queue = GcpPubSubQueue.from_obj(
            {
                'name': 'orders',
                'project': 'example-project',
                'topic': 'orders-topic',
                'subscription': 'etlplus',
                'options': {'ack_deadline_seconds': 30},
            },
        )

        assert isinstance(queue, QueueConfigProtocol)
        actual = (
            queue.to_connector_options()
            if field_name == 'connector_options'
            else getattr(queue, field_name)
        )
        assert actual == expected
