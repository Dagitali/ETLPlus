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

    def test_from_obj_rejects_missing_project(self) -> None:
        """Test Google Cloud Pub/Sub metadata requires a project."""
        with pytest.raises(ValueError, match='requires "project"'):
            GcpPubSubQueue.from_obj(
                {'name': 'orders', 'subscription': 'etlplus'},
            )

    def test_from_obj_rejects_missing_target(self) -> None:
        """Test Google Cloud Pub/Sub metadata requires a topic or subscription."""
        with pytest.raises(ValueError, match='requires "topic" or "subscription"'):
            GcpPubSubQueue.from_obj(
                {'name': 'orders', 'project': 'example-project'},
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
