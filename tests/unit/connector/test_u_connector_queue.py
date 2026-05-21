"""
:mod:`tests.unit.connector.test_u_connector_queue` module.

Unit tests for :mod:`etlplus.connector._queue`.
"""

from __future__ import annotations

from typing import cast

import pytest

from etlplus.connector._enums import DataConnectorType
from etlplus.connector._queue import ConnectorQueue
from etlplus.queue import QueueService
from etlplus.queue import QueueType

from .pytest_connector_support import QueueConnectorProviderCase
from .pytest_connector_support import assert_connector_fields
from .pytest_connector_support import get_queue_connector_provider_case

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


AWS_SQS_CASE = get_queue_connector_provider_case('aws-sqs')
AZURE_SERVICE_BUS_CASE = get_queue_connector_provider_case('azure-service-bus')
GCP_PUBSUB_CASE = get_queue_connector_provider_case('gcp-pubsub')
RABBITMQ_CASE = get_queue_connector_provider_case('rabbitmq')
REDIS_STREAMS_CASE = get_queue_connector_provider_case('redis-streams')


# SECTION: FIXTURES ========================================================= #


@pytest.fixture(
    name='queue_provider_case',
    params=[
        pytest.param(AWS_SQS_CASE, id='aws-sqs'),
        pytest.param(AZURE_SERVICE_BUS_CASE, id='azure-service-bus'),
        pytest.param(GCP_PUBSUB_CASE, id='gcp-pubsub'),
        pytest.param(RABBITMQ_CASE, id='rabbitmq-alias'),
        pytest.param(REDIS_STREAMS_CASE, id='redis-streams-alias'),
    ],
)
def queue_provider_case_fixture(
    request: pytest.FixtureRequest,
) -> QueueConnectorProviderCase:
    """Return one canonical queue provider case."""
    return request.param


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
        ('payload', 'expected_queue_type'),
        [
            pytest.param(
                {
                    'name': 'events',
                    'type': 'queue',
                    'service': 'amqp',
                    'queue_name': 'events.fifo',
                    'options': {'url': 'amqp://guest:guest@localhost:5672/%2f'},
                },
                QueueType.STANDARD,
                id='fifo-suffix-does-not-imply-non-sqs-fifo',
            ),
            pytest.param(
                {
                    'name': 'fifo_events',
                    'type': 'queue',
                    'service': 'amqp',
                    'queue_name': 'events',
                    'queue_type': 'fifo',
                },
                QueueType.FIFO,
                id='explicit-non-sqs-fifo-skips-sqs-name-rule',
            ),
        ],
    )
    def test_fifo_rules_are_sqs_specific(
        self,
        payload: dict[str, object],
        expected_queue_type: QueueType,
    ) -> None:
        """Test that non-SQS FIFO-like metadata does not use SQS naming rules."""
        connector = ConnectorQueue.from_obj(payload)

        assert connector.service is QueueService.AMQP
        assert connector.queue_type is expected_queue_type

    @pytest.mark.parametrize(
        ('payload', 'expected_queue_name', 'expected_service'),
        [
            pytest.param(
                {
                    'name': 'events',
                    'type': 'queue',
                    'queue_name': None,
                    'queue': 'events.fifo',
                },
                'events.fifo',
                QueueService.AWS_SQS,
                id='queue-alias-used-when-primary-empty',
            ),
            pytest.param(
                {
                    'name': 'events',
                    'type': 'queue',
                    'service': None,
                },
                None,
                QueueService.AWS_SQS,
                id='service-none-uses-default',
            ),
        ],
    )
    def test_from_obj_normalizes_queue_alias_and_service_defaults(
        self,
        payload: dict[str, object],
        expected_queue_name: str | None,
        expected_service: QueueService,
    ) -> None:
        """Queue aliases and missing-like services should normalize consistently."""
        connector = ConnectorQueue.from_obj(payload)

        assert connector.queue_name == expected_queue_name
        assert connector.service is expected_service

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
                    'service': QueueService.AWS_SQS,
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
                    'service': QueueService.AWS_SQS,
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
        ('payload', 'expected_options'),
        [
            pytest.param(
                {
                    'name': 'events',
                    'type': 'queue',
                    'service': 'aws-sqs',
                    'queue_name': 'events',
                    'url': 'https://sqs.us-east-1.amazonaws.com/123/events',
                    'region': 'us-east-1',
                    'options': {
                        'queue_name': 'stale',
                        'url': 'stale',
                        'region': 'stale',
                    },
                },
                {
                    'service': 'aws-sqs',
                    'queue_type': 'standard',
                    'queue_name': 'events',
                    'url': 'https://sqs.us-east-1.amazonaws.com/123/events',
                    'region': 'us-east-1',
                },
                id='top-level-overrides-options',
            ),
            pytest.param(
                {
                    'name': 'rabbit',
                    'type': 'queue',
                    'service': 'amqp',
                    'options': {
                        'url': 'amqp://guest:guest@localhost:5672/%2f',
                    },
                },
                {
                    'service': 'amqp',
                    'url': 'amqp://guest:guest@localhost:5672/%2f',
                },
                id='missing-top-level-preserves-options',
            ),
        ],
    )
    def test_to_queue_config_field_precedence(
        self,
        payload: dict[str, object],
        expected_options: dict[str, object],
    ) -> None:
        """Test queue config conversion field precedence rules."""
        assert (
            ConnectorQueue.from_obj(payload).to_queue_config().to_connector_options()
            == expected_options
        )

    def test_to_queue_config_rejects_unsupported_service(self) -> None:
        """Test queue config conversion rejects impossible service values."""
        connector = ConnectorQueue(
            name='events',
            service=cast(QueueService, 'unsupported'),
        )

        with pytest.raises(ValueError, match='Unsupported queue service'):
            connector.to_queue_config()

    def test_to_queue_config_returns_provider_specific_config(
        self,
        queue_provider_case: QueueConnectorProviderCase,
    ) -> None:
        """Test conversion into provider-specific queue config objects."""
        queue_config = ConnectorQueue.from_obj(
            queue_provider_case.connector_payload(),
        ).to_queue_config()

        assert isinstance(queue_config, queue_provider_case.expected_config_cls)
        assert queue_config.to_connector_options() == (
            queue_provider_case.expected_queue_options
        )
