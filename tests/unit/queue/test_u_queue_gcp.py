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

    def test_from_obj_normalizes_optional_string_fields(self) -> None:
        """Test Google Cloud Pub/Sub metadata trims optional target fields."""
        queue = GcpPubSubQueue.from_obj(
            {
                'name': 'orders',
                'project': '  example-project  ',
                'topic': '  orders-topic  ',
                'subscription': '  etlplus  ',
            },
        )

        assert queue.project == 'example-project'
        assert queue.topic == 'orders-topic'
        assert queue.subscription == 'etlplus'

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

    def test_from_obj_requires_name(self) -> None:
        """Test that :meth:`from_obj` requires a queue metadata name."""
        with pytest.raises(TypeError, match='GcpPubSubQueue requires a "name"'):
            GcpPubSubQueue.from_obj(
                {'project': 'example-project', 'subscription': 'etlplus'},
            )

    def test_from_obj_returns_connector_options(self) -> None:
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
        assert queue.service is QueueService.GCP_PUBSUB
        assert queue.to_connector_options() == {
            'ack_deadline_seconds': 30,
            'service': 'gcp-pubsub',
            'project': 'example-project',
            'topic': 'orders-topic',
            'subscription': 'etlplus',
        }

    def test_to_connector_options_omits_empty_optional_fields(self) -> None:
        """Test that empty optional Pub/Sub metadata does not appear in options."""
        assert GcpPubSubQueue.from_obj(
            {'name': 'orders', 'project': 'example-project', 'topic': 'orders-topic'},
        ).to_connector_options() == {
            'service': 'gcp-pubsub',
            'project': 'example-project',
            'topic': 'orders-topic',
        }
