"""
:mod:`tests.unit.queue.test_u_queue_aws` module.

Unit tests for :mod:`etlplus.queue._aws`.
"""

from __future__ import annotations

import pytest

from etlplus.queue import AwsSqsQueue
from etlplus.queue import QueueConfigProtocol
from etlplus.queue import QueueService
from etlplus.queue import QueueType

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestAwsSqsQueue:
    """Unit tests for :class:`etlplus.queue.AwsSqsQueue`."""

    def test_fifo_queue_name_must_end_with_fifo_suffix(self) -> None:
        """Test that explicit FIFO queues require the SQS ``.fifo`` suffix."""
        with pytest.raises(ValueError, match='must end with ".fifo"'):
            AwsSqsQueue.from_obj({'name': 'events', 'queue_type': 'fifo'})

    @pytest.mark.parametrize(
        ('payload', 'expected_type'),
        [
            pytest.param({'name': 'events'}, QueueType.STANDARD, id='standard'),
            pytest.param({'name': 'events.fifo'}, QueueType.FIFO, id='fifo-suffix'),
            pytest.param(
                {'name': 'events.fifo', 'queue_type': 'fifo'},
                QueueType.FIFO,
                id='explicit-fifo',
            ),
        ],
    )
    def test_from_obj_normalizes_queue_fields(
        self,
        payload: dict[str, object],
        expected_type: QueueType,
    ) -> None:
        """Test that :meth:`from_obj` normalizes SQS queue metadata."""
        connector = AwsSqsQueue.from_obj(
            {
                **payload,
                'url': 123,
                'arn': False,
                'region': 'us-east-1',
                'delay_seconds': '5',
                'max_messages': '10',
                'message_retention_period': 345600,
                'visibility_timeout': '30',
                'wait_time_seconds': '20',
                'dead_letter_queue_arn': 'arn:aws:sqs:us-east-1:123:dead',
                'attributes': {'VisibilityTimeout': '30'},
            },
        )

        assert connector.service is QueueService.AWS_SQS
        assert isinstance(connector, QueueConfigProtocol)
        assert connector.queue_type is expected_type
        assert connector.url == '123'
        assert connector.arn == 'False'
        assert connector.region == 'us-east-1'
        assert connector.delay_seconds == 5
        assert connector.max_messages == 10
        assert connector.message_retention_period == 345600
        assert connector.visibility_timeout == 30
        assert connector.wait_time_seconds == 20
        assert connector.dead_letter_queue_arn == 'arn:aws:sqs:us-east-1:123:dead'
        assert connector.attributes == {'VisibilityTimeout': '30'}

    def test_from_obj_requires_name(self) -> None:
        """Test that :meth:`from_obj` requires a queue name."""
        with pytest.raises(TypeError, match='AwsSqsQueue requires a "name"'):
            AwsSqsQueue.from_obj({'queue_type': 'fifo'})

    @pytest.mark.parametrize(
        ('payload', 'expected_fifo', 'expected_standard'),
        [
            pytest.param({'name': 'events'}, False, True, id='standard'),
            pytest.param({'name': 'events.fifo'}, True, False, id='fifo'),
        ],
    )
    def test_queue_type_getters(
        self,
        payload: dict[str, object],
        expected_fifo: bool,
        expected_standard: bool,
    ) -> None:
        """Test that queue type getters expose normalized SQS semantics."""
        queue = AwsSqsQueue.from_obj(payload)

        assert queue.is_fifo is expected_fifo
        assert queue.is_standard is expected_standard

    def test_from_obj_rejects_boolean_integer_metadata(self) -> None:
        """Test that boolean values are not accepted as integer metadata."""
        with pytest.raises(TypeError, match='"visibility_timeout" must be an integer'):
            AwsSqsQueue.from_obj({'name': 'events', 'visibility_timeout': True})

    def test_from_obj_rejects_invalid_integer_metadata(self) -> None:
        """Test that integer SQS metadata fields reject non-integer values."""
        with pytest.raises(TypeError, match='"visibility_timeout" must be an integer'):
            AwsSqsQueue.from_obj(
                {
                    'name': 'events',
                    'visibility_timeout': 'not-an-int',
                },
            )

    @pytest.mark.parametrize(
        ('field_name', 'value'),
        [
            pytest.param('delay_seconds', 901, id='delay-seconds'),
            pytest.param('max_messages', 11, id='max-messages'),
            pytest.param(
                'message_retention_period',
                1_209_601,
                id='message-retention-period',
            ),
            pytest.param('visibility_timeout', 43_201, id='visibility-timeout'),
            pytest.param('wait_time_seconds', 21, id='wait-time-seconds'),
        ],
    )
    def test_from_obj_rejects_out_of_range_integer_metadata(
        self,
        field_name: str,
        value: int,
    ) -> None:
        """Test that bounded SQS metadata fields enforce AWS limits."""
        with pytest.raises(ValueError, match=f'"{field_name}" must be between'):
            AwsSqsQueue.from_obj(
                {
                    'name': 'events',
                    field_name: value,
                },
            )

    def test_standard_queue_rejects_fifo_only_metadata(self) -> None:
        """Test that standard queues reject FIFO-only message metadata."""
        with pytest.raises(ValueError, match='FIFO fields require'):
            AwsSqsQueue.from_obj(
                {
                    'name': 'events',
                    'message_group_id': 'events',
                },
            )

    def test_to_connector_options_includes_arn(self) -> None:
        """Test that SQS ARN metadata is preserved in connector options."""
        arn = 'arn:aws:sqs:us-east-1:123:events'

        assert (
            AwsSqsQueue.from_obj({'name': 'events', 'arn': arn}).to_connector_options()[
                'arn'
            ]
            == arn
        )

    def test_to_connector_options_omits_empty_optional_fields(self) -> None:
        """Test that empty optional SQS metadata does not appear in options."""
        assert AwsSqsQueue.from_obj({'name': 'events'}).to_connector_options() == {
            'service': 'aws-sqs',
            'queue_type': 'standard',
            'queue_name': 'events',
        }

    def test_to_connector_options_returns_plain_mapping(self) -> None:
        """Test that queue metadata can be exposed as connector options."""
        queue = AwsSqsQueue.from_obj(
            {
                'name': 'events.fifo',
                'url': 'https://sqs.us-east-1.amazonaws.com/123/events.fifo',
                'region': 'us-east-1',
                'visibility_timeout': 30,
                'wait_time_seconds': 20,
                'content_based_deduplication': True,
                'message_group_id': 'events',
                'attributes': {'ContentBasedDeduplication': 'true'},
            },
        )

        assert queue.to_connector_options() == {
            'ContentBasedDeduplication': 'true',
            'service': 'aws-sqs',
            'queue_type': 'fifo',
            'queue_name': 'events.fifo',
            'url': 'https://sqs.us-east-1.amazonaws.com/123/events.fifo',
            'region': 'us-east-1',
            'visibility_timeout': 30,
            'wait_time_seconds': 20,
            'content_based_deduplication': True,
            'message_group_id': 'events',
        }

    def test_modeled_fields_override_attributes(self) -> None:
        """Test top-level SQS fields take precedence over duplicate attributes."""
        queue = AwsSqsQueue.from_obj(
            {
                'name': 'events',
                'region': 'us-east-1',
                'attributes': {
                    'service': 'wrong',
                    'queue_type': 'wrong',
                    'queue_name': 'stale',
                    'region': 'stale',
                },
            },
        )

        assert queue.to_connector_options() == {
            'service': 'aws-sqs',
            'queue_type': 'standard',
            'queue_name': 'events',
            'region': 'us-east-1',
        }
