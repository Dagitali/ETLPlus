"""
:mod:`tests.unit.queue.test_u_queue_options` module.

Unit tests for shared queue option serialization behavior.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest

from etlplus.queue import AmqpQueue
from etlplus.queue import AwsSqsQueue
from etlplus.queue import AzureServiceBusQueue
from etlplus.queue import GcpPubSubQueue
from etlplus.queue import QueueConfig
from etlplus.queue import RedisQueue
from etlplus.utils._types import StrAnyMap

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestQueueOptions:
    """Unit tests for shared queue option serialization."""

    @pytest.mark.parametrize(
        ('factory', 'payload', 'expected'),
        [
            pytest.param(
                AmqpQueue.from_obj,
                {
                    'name': 'orders',
                    'host': 'localhost',
                    'options': {
                        'service': 'wrong',
                        'host': 'stale',
                    },
                },
                {
                    'service': 'amqp',
                    'host': 'localhost',
                },
                id='amqp',
            ),
            pytest.param(
                AwsSqsQueue.from_obj,
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
                {
                    'service': 'aws-sqs',
                    'queue_type': 'standard',
                    'queue_name': 'events',
                    'region': 'us-east-1',
                },
                id='aws-sqs',
            ),
            pytest.param(
                AzureServiceBusQueue.from_obj,
                {
                    'name': 'orders',
                    'queue_name': 'orders',
                    'options': {
                        'service': 'wrong',
                        'queue_name': 'stale',
                    },
                },
                {
                    'service': 'azure-service-bus',
                    'queue_name': 'orders',
                },
                id='azure-service-bus',
            ),
            pytest.param(
                GcpPubSubQueue.from_obj,
                {
                    'name': 'orders',
                    'project': 'example-project',
                    'topic': 'orders-topic',
                    'options': {
                        'service': 'wrong',
                        'project': 'stale',
                        'topic': 'stale',
                    },
                },
                {
                    'service': 'gcp-pubsub',
                    'project': 'example-project',
                    'topic': 'orders-topic',
                },
                id='gcp-pubsub',
            ),
            pytest.param(
                RedisQueue.from_obj,
                {
                    'name': 'orders',
                    'key': 'events',
                    'database': 1,
                    'options': {
                        'service': 'wrong',
                        'key': 'stale',
                        'database': 0,
                    },
                },
                {
                    'service': 'redis',
                    'key': 'events',
                    'database': 1,
                },
                id='redis',
            ),
        ],
    )
    def test_modeled_fields_override_provider_options(
        self,
        factory: Callable[[StrAnyMap], QueueConfig],
        payload: StrAnyMap,
        expected: dict[str, object],
    ) -> None:
        """Test top-level queue fields take precedence over duplicate options."""
        assert factory(payload).to_connector_options() == expected
