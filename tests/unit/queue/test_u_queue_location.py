"""
:mod:`tests.unit.queue.test_u_queue_location` module.

Unit tests for :mod:`etlplus.queue._location`.
"""

from __future__ import annotations

import pytest

from etlplus.queue import QueueLocation
from etlplus.queue import QueueService

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestQueueLocation:
    """Unit tests for :class:`etlplus.queue.QueueLocation`."""

    @pytest.mark.parametrize(
        ('value', 'service', 'authority', 'path'),
        [
            (
                'aws-sqs://us-east-1/events.fifo',
                QueueService.AWS_SQS,
                'us-east-1',
                'events.fifo',
            ),
            (
                'redis://localhost:6379/0/events',
                QueueService.REDIS,
                'localhost:6379',
                '0/events',
            ),
            (
                'sqs://us-east-1/events',
                QueueService.AWS_SQS,
                'us-east-1',
                'events',
            ),
            (
                'redis://localhost:6379/0/orders%2Fcreated',
                QueueService.REDIS,
                'localhost:6379',
                '0/orders/created',
            ),
        ],
    )
    def test_from_value_parses_queue_uri(
        self,
        value: str,
        service: QueueService,
        authority: str,
        path: str,
    ) -> None:
        """Test that queue URIs parse into normalized location parts."""
        location = QueueLocation.from_value(value)

        assert location.raw == value
        assert location.service is service
        assert location.authority == authority
        assert location.path == path

    @pytest.mark.parametrize(
        ('value', 'match'),
        [
            pytest.param('', 'cannot be empty', id='empty'),
            pytest.param('events', 'requires a service scheme', id='missing-scheme'),
            pytest.param('aws-sqs://us-east-1', 'requires a queue path', id='no-path'),
        ],
    )
    def test_from_value_rejects_invalid_queue_uri(
        self,
        value: str,
        match: str,
    ) -> None:
        """Test that invalid queue URIs raise descriptive errors."""
        with pytest.raises(ValueError, match=match):
            QueueLocation.from_value(value)
