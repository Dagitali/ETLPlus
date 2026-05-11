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
        ('value', 'expected'),
        [
            pytest.param(
                'aws-sqs://us-east-1/events.fifo',
                {
                    'service': QueueService.AWS_SQS,
                    'authority': 'us-east-1',
                    'path': 'events.fifo',
                },
                id='aws-sqs',
            ),
            pytest.param(
                'redis://localhost:6379/0/events',
                {
                    'service': QueueService.REDIS,
                    'authority': 'localhost:6379',
                    'path': '0/events',
                },
                id='redis',
            ),
            pytest.param(
                'sqs://us-east-1/events',
                {
                    'service': QueueService.AWS_SQS,
                    'authority': 'us-east-1',
                    'path': 'events',
                },
                id='sqs-alias',
            ),
        ],
    )
    def test_from_value_parses_queue_uri(
        self,
        value: str,
        expected: dict[str, object],
    ) -> None:
        """Test that queue URIs parse into normalized location parts."""
        location = QueueLocation.from_value(value)

        assert location.raw == value
        assert location.service is expected['service']
        assert location.authority == expected['authority']
        assert location.path == expected['path']

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
