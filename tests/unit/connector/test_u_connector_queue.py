"""
:mod:`tests.unit.connector.test_u_connector_queue` module.

Unit tests for :mod:`etlplus.connector._queue`.
"""

from __future__ import annotations

import pytest

from etlplus.connector._enums import DataConnectorType
from etlplus.connector._queue import ConnectorQueue
from etlplus.queue import QueueService
from etlplus.queue import QueueType

from .pytest_connector_support import assert_connector_fields

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestConnectorQueue:
    """Unit tests for :class:`ConnectorQueue`."""

    def test_fifo_queue_name_must_end_with_fifo_suffix(self) -> None:
        """Test that explicit FIFO queue names require the SQS suffix."""
        with pytest.raises(ValueError, match='must end with ".fifo"'):
            ConnectorQueue.from_obj(
                {
                    'name': 'fifo_events',
                    'type': 'queue',
                    'queue_name': 'events',
                    'queue_type': 'fifo',
                },
            )

    @pytest.mark.parametrize(
        ('payload', 'expected'),
        [
            pytest.param(
                {
                    'name': 'events_queue',
                    'type': 'queue',
                    'service': 'aws-sqs',
                    'queue_name': 'events',
                    'url': 'https://sqs.us-east-1.amazonaws.com/123/events',
                    'region': 'us-east-1',
                    'options': {'visibility_timeout': 30},
                },
                {
                    'type': DataConnectorType.QUEUE,
                    'name': 'events_queue',
                    'service': QueueService.SQS,
                    'queue_type': QueueType.STANDARD,
                    'queue_name': 'events',
                    'url': 'https://sqs.us-east-1.amazonaws.com/123/events',
                    'region': 'us-east-1',
                    'options': {'visibility_timeout': 30},
                },
                id='standard-sqs',
            ),
            pytest.param(
                {
                    'name': 'fifo_events',
                    'type': 'queue',
                    'queue': 'events.fifo',
                    'region': 123,
                    'options': [('visibility_timeout', 30)],
                },
                {
                    'type': DataConnectorType.QUEUE,
                    'name': 'fifo_events',
                    'service': QueueService.SQS,
                    'queue_type': QueueType.FIFO,
                    'queue_name': 'events.fifo',
                    'url': None,
                    'region': '123',
                    'options': {},
                },
                id='fifo-sqs',
            ),
        ],
    )
    def test_from_obj_normalizes_queue_fields(
        self,
        payload: dict[str, object],
        expected: dict[str, object],
    ) -> None:
        """Test that :meth:`from_obj` preserves standard queue fields."""
        connector = ConnectorQueue.from_obj(payload)

        assert_connector_fields(connector, expected)

    @pytest.mark.parametrize(
        'payload',
        [
            pytest.param({'type': 'queue'}, id='missing-name'),
            pytest.param({'name': None, 'type': 'queue'}, id='non-string-name'),
        ],
    )
    def test_from_obj_requires_name(
        self,
        payload: dict[str, object],
    ) -> None:
        """Test that :meth:`from_obj` rejects missing or invalid names."""
        with pytest.raises(TypeError, match='ConnectorQueue requires a "name"'):
            ConnectorQueue.from_obj(payload)
