"""
:mod:`tests.unit.connector.pytest_connector_support` module.

Shared helpers for pytest-based unit tests of :mod:`etlplus.connector`.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

# SECTION: DATA CLASSES ===================================================== #


@dataclass(frozen=True, slots=True)
class QueueConnectorProviderCase:
    """Canonical connector-test data for one queue provider scenario."""

    input_service: str
    connector_name: str
    top_level_fields: dict[str, object]
    options: dict[str, object]
    expected_queue_options: dict[str, object]

    def connector_payload(self, **extra_fields: object) -> dict[str, object]:
        """Return one canonical queue connector payload for this provider."""
        payload: dict[str, object] = {
            'name': self.connector_name,
            'type': 'queue',
            'service': self.input_service,
            **self.top_level_fields,
        }
        if self.options:
            payload['options'] = dict(self.options)
        payload.update(extra_fields)
        return payload

    def expected_connector_attrs(self, **overrides: object) -> dict[str, object]:
        """Return normalized connector attrs for this provider case."""
        expected: dict[str, object] = {
            'name': self.connector_name,
            'service': self.expected_queue_options['service'],
        }
        for field_name in ('queue_name', 'url', 'region'):
            expected[field_name] = self.top_level_fields.get(field_name)
        expected.update(overrides)
        return expected


# SECTION: INTERNAL CONSTANTS =============================================== #


_QUEUE_CONNECTOR_PROVIDER_CASES: dict[str, QueueConnectorProviderCase] = {
    'aws-sqs': QueueConnectorProviderCase(
        input_service='aws-sqs',
        connector_name='events',
        top_level_fields={
            'queue_name': 'events.fifo',
            'region': 'us-east-1',
        },
        options={'visibility_timeout': 30},
        expected_queue_options={
            'service': 'aws-sqs',
            'queue_type': 'fifo',
            'queue_name': 'events.fifo',
            'region': 'us-east-1',
            'visibility_timeout': 30,
        },
    ),
    'azure-service-bus': QueueConnectorProviderCase(
        input_service='azure-service-bus',
        connector_name='servicebus',
        top_level_fields={'queue_name': 'orders'},
        options={'namespace': 'example-bus'},
        expected_queue_options={
            'service': 'azure-service-bus',
            'namespace': 'example-bus',
            'queue_name': 'orders',
        },
    ),
    'gcp-pubsub': QueueConnectorProviderCase(
        input_service='gcp-pubsub',
        connector_name='pubsub',
        top_level_fields={},
        options={
            'project': 'example-project',
            'subscription': 'etlplus',
        },
        expected_queue_options={
            'service': 'gcp-pubsub',
            'project': 'example-project',
            'subscription': 'etlplus',
        },
    ),
    'rabbitmq': QueueConnectorProviderCase(
        input_service='rabbitmq',
        connector_name='rabbit',
        top_level_fields={},
        options={
            'url': 'amqp://guest:guest@localhost:5672/%2f',
            'routing_key': 'orders.created',
        },
        expected_queue_options={
            'service': 'amqp',
            'url': 'amqp://guest:guest@localhost:5672/%2f',
            'routing_key': 'orders.created',
        },
    ),
    'redis-streams': QueueConnectorProviderCase(
        input_service='redis-streams',
        connector_name='redis',
        top_level_fields={'queue_name': 'orders'},
        options={'database': '2'},
        expected_queue_options={
            'service': 'redis',
            'key': 'orders',
            'database': 2,
        },
    ),
}


# SECTION: FUNCTIONS ======================================================== #

def assert_connector_fields(
    actual: object,
    expected: Mapping[str, object],
) -> None:
    """Assert that *actual* exposes the expected field values."""
    for field, value in expected.items():
        assert getattr(actual, field) == value


def get_queue_connector_provider_case(
    service: str,
) -> QueueConnectorProviderCase:
    """Return one canonical connector-test case for *service*."""
    return _QUEUE_CONNECTOR_PROVIDER_CASES[service]
